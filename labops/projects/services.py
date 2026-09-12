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
    require(not Project.objects.filter(code=code).exclude(pk=id).exists(),'DUPLICATE_CODE','项目编号已存在',409,'code')
    owner=obj(User,data.get('owner_id',str(project.owner_id) if project else str(user.id)))
    require(owner.is_active and bool(roles(owner)&{'ADMIN','MANAGER'}),'INVALID_OWNER','负责人必须是有效项目经理或管理员')
    if not project and 'ADMIN' not in roles(user): require(owner.id==user.id,'FORBIDDEN','项目经理只能创建自己负责的项目',403)
    start=day(data.get('start_date',str(project.start_date or '') if project else ''),'start_date',True)
    due=day(data.get('due_date',str(project.due_date or '') if project else ''),'due_date',True)
    require(not(start and due and due<start),'INVALID_DATE','截止日期不能早于开始日期',422,'due_date')
    budget=data.get('budget_amount',project.budget_amount if project else None)
    if budget not in [None,'']:
        budget=qty(budget,'budget_amount',False)
        require(budget==budget.quantize(Decimal('.01')),'INVALID_NUMBER','预算最多两位小数',422,'budget_amount')
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
        require(target in PROJECT_NEXT.get(p.status,[]),'INVALID_TRANSITION','项目状态转换不允许')
        if target=='COMPLETED': require(not p.tasks.exclude(status__in=['DONE','CANCELLED']).exists(),'TASKS_UNFINISHED','请先完成或取消所有任务')
        p.status=target
    elif action=='members':
        require_open(p); member=obj(User,data.get('user_id'))
        if data.get('remove'):
            require(member.id!=p.owner_id,'OWNER_REQUIRED','请先移交项目负责人')
            require(not p.tasks.filter(assignee=member).exclude(status__in=['DONE','CANCELLED']).exists(),'TASKS_ASSIGNED','请先移交该成员未完成的任务')
            ProjectMember.objects.filter(project=p,user=member).delete()
        else:
            require(member.is_active and bool(roles(member)&{'ADMIN','MANAGER','TECH'}),'INVALID_MEMBER','请选择有效项目成员')
            ProjectMember.objects.get_or_create(project=p,user=member)
    else: fail('INVALID_ACTION','操作不存在',404)
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
        require(t.status not in ['DONE','CANCELLED'],'TASK_CLOSED','已结束任务只读')
    assignee_id=data.get('assignee_id',str(t.assignee_id) if t and t.assignee_id else '')
    assignee=obj(User,assignee_id) if assignee_id else None
    if assignee: require(assignee.is_active and p.members.filter(pk=assignee.id).exists(),'INVALID_ASSIGNEE','负责人必须是有效项目成员',422,'assignee_id')
    if t and t.status=='IN_PROGRESS': require(assignee is not None,'ASSIGNEE_REQUIRED','进行中任务必须有负责人')
    priority=data.get('priority',t.priority if t else 'NORMAL')
    require(priority in ['LOW','NORMAL','HIGH'],'INVALID_PRIORITY','优先级无效')
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
    require(action=='transition','INVALID_ACTION','操作不存在',404)
    if not roles(user)&{'ADMIN','MANAGER'}: require(t.assignee_id==user.id,'FORBIDDEN','实验员只能更新自己负责的任务',403)
    version(t,data); target=data.get('target_status')
    require(target in TASK_NEXT.get(t.status,[]),'INVALID_TRANSITION','任务状态转换不允许')
    before=snapshot(t)
    if target=='IN_PROGRESS': require(t.assignee and t.assignee.is_active and t.project.members.filter(pk=t.assignee_id).exists(),'ASSIGNEE_REQUIRED','进入进行中前请分配有效成员')
    t.blocked_reason=text(data.get('reason',''),'reason',2000) if target=='BLOCKED' else ''
    t.status=target; save_change(user,t,rid,before,'TRANSITION')
    if target=='DONE':
        from labops.operations.services import emit
        emit('TASK_DONE',t,'任务已完成',t.title,[t.assignee_id,t.project.owner_id])
    return t
