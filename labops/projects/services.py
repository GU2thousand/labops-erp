from labops.common import *
from labops.models import Project,ProjectMember,Task,TaskComment,User
PROJECT_NEXT={'DRAFT':['ACTIVE','CANCELLED'],'ACTIVE':['ON_HOLD','COMPLETED','CANCELLED'],'ON_HOLD':['ACTIVE'],'COMPLETED':['ARCHIVED'],'ARCHIVED':[],'CANCELLED':[]}
TASK_NEXT={'TODO':['IN_PROGRESS','BLOCKED','CANCELLED'],'IN_PROGRESS':['DONE','BLOCKED','CANCELLED'],'BLOCKED':['TODO','IN_PROGRESS','CANCELLED'],'DONE':[],'CANCELLED':[]}

def manager(user,project):
    allow(user,'ADMIN','MANAGER'); project_scope(user,project,True)

@atomic_command
def write_project(user,data,rid,id=None):
    allow(user,'ADMIN','MANAGER')
    project=obj(Project,id) if id else None
    if project:
        manager(user,project); require_open(project); version(project,data)
    code=text(data.get('code',project.code if project else ''),'code',64).upper()
    require(not Project.objects.filter(code=code).exclude(pk=id).exists(),'DUPLICATE_CODE','Project code already exists',409,'code')
    owner=obj(User,data.get('owner_id',str(project.owner_id) if project else str(user.id)))
    require(owner.is_active and bool(roles(owner)&{'ADMIN','MANAGER'}),'INVALID_OWNER','The owner must be an active project manager or administrator')
    if not project and 'ADMIN' not in roles(user): require(owner.id==user.id,'FORBIDDEN','Project managers can only create projects they own',403)
    start=day(data.get('start_date',str(project.start_date or '') if project else ''),'start_date',True)
    due=day(data.get('due_date',str(project.due_date or '') if project else ''),'due_date',True)
    require(not(start and due and due<start),'INVALID_DATE','Due date cannot precede the start date',422,'due_date')
    budget=data.get('budget_amount',project.budget_amount if project else None)
    if budget not in [None,'']:
        budget=qty(budget,'budget_amount',False)
        require(budget==budget.quantize(Decimal('.01')),'INVALID_NUMBER','Budget supports at most two decimal places',422,'budget_amount')
    else: budget=None
    values=dict(code=code,name=text(data.get('name',project.name if project else ''),'name'),owner=owner,start_date=start,due_date=due,budget_amount=budget)
    if project:
        before=snapshot(project)
        for k,v in values.items(): setattr(project,k,v)
        save_change(user,project,rid,before)
    else: project=new(Project,user,rid,**values)
    ProjectMember.objects.get_or_create(project=project,user=owner)
    return project

@atomic_command
def project_action(user,id,action,data,rid):
    p=obj(Project,id); manager(user,p); version(p,data)
    before=snapshot(p)
    if action=='transition':
        target=data.get('target_status')
        require(target in PROJECT_NEXT.get(p.status,[]),'INVALID_TRANSITION','This project status transition is not allowed')
        if target=='COMPLETED': require(not p.tasks.exclude(status__in=['DONE','CANCELLED']).exists(),'TASKS_UNFINISHED','Complete or cancel all tasks first')
        p.status=target
    elif action=='members':
        require_open(p); member=obj(User,data.get('user_id'))
        if data.get('remove'):
            require(member.id!=p.owner_id,'OWNER_REQUIRED','Transfer project ownership first')
            require(not p.tasks.filter(assignee=member).exclude(status__in=['DONE','CANCELLED']).exists(),'TASKS_ASSIGNED','Reassign this member’s unfinished tasks first')
            ProjectMember.objects.filter(project=p,user=member).delete()
        else:
            require(member.is_active and bool(roles(member)&{'ADMIN','MANAGER','TECH'}),'INVALID_MEMBER','Select an active project member')
            ProjectMember.objects.get_or_create(project=p,user=member)
    else: fail('INVALID_ACTION','Action not found',404)
    save_change(user,p,rid,before,action.upper(),str(data.get('user_id','')))
    return p

@atomic_command
def write_task(user,data,rid,id=None):
    allow(user,'ADMIN','MANAGER','TECH')
    t=obj(Task,id) if id else None
    p=t.project if t else obj(Project,data.get('project_id'))
    project_scope(user,p,True); require_open(p)
    if t:
        manager(user,p); version(t,data)
        require(t.status not in ['DONE','CANCELLED'],'TASK_CLOSED','Closed tasks are read-only')
    assignee_id=data.get('assignee_id',str(t.assignee_id) if t and t.assignee_id else '')
    assignee=obj(User,assignee_id) if assignee_id else None
    if assignee: require(assignee.is_active and p.members.filter(pk=assignee.id).exists(),'INVALID_ASSIGNEE','The assignee must be an active project member',422,'assignee_id')
    if t and t.status=='IN_PROGRESS': require(assignee is not None,'ASSIGNEE_REQUIRED','In-progress tasks require an assignee')
    priority=data.get('priority',t.priority if t else 'NORMAL')
    require(priority in ['LOW','NORMAL','HIGH'],'INVALID_PRIORITY','Invalid priority')
    fields=dict(title=text(data.get('title',t.title if t else ''),'title'),description=text(data.get('description',t.description if t else ''),'description',10000,False),assignee=assignee,priority=priority,due_date=day(data.get('due_date',str(t.due_date or '') if t else ''),'due_date',True))
    if not t: return new(Task,user,rid,project=p,**fields)
    before=snapshot(t)
    for k,v in fields.items(): setattr(t,k,v)
    save_change(user,t,rid,before)
    return t

@atomic_command
def task_action(user,id,action,data,rid):
    t=obj(Task,id); project_scope(user,t.project,True); require_open(t.project)
    allow(user,'ADMIN','MANAGER','TECH')
    if action=='comments':
        c=TaskComment.objects.create(task=t,author=user,body=text(data.get('body',''),'body',5000))
        audit(user,c,'COMMENT',rid); return c
    require(action=='transition','INVALID_ACTION','Action not found',404)
    if not roles(user)&{'ADMIN','MANAGER'}: require(t.assignee_id==user.id,'FORBIDDEN','Technicians can only update tasks assigned to them',403)
    version(t,data); target=data.get('target_status')
    require(target in TASK_NEXT.get(t.status,[]),'INVALID_TRANSITION','This task status transition is not allowed')
    before=snapshot(t)
    if target=='IN_PROGRESS': require(t.assignee and t.assignee.is_active and t.project.members.filter(pk=t.assignee_id).exists(),'ASSIGNEE_REQUIRED','Assign an active member before starting the task')
    t.blocked_reason=text(data.get('reason',''),'reason',2000) if target=='BLOCKED' else ''
    t.status=target; save_change(user,t,rid,before,'TRANSITION')
    if target=='DONE':
        from labops.operations.services import emit
        emit('TASK_DONE',t,'Task completed',t.title,[t.assignee_id,t.project.owner_id])
    return t
