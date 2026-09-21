import json, uuid, time
from pathlib import Path
from datetime import date, timedelta
from decimal import Decimal
import requests
BASE='http://localhost:8001'
results=[]
def check(name, condition):
    assert condition, name
    results.append({'name':name,'passed':True})
    Path('output/playwright/api-results.json').write_text(json.dumps(results,indent=2))
def login(email):
    s=requests.Session(); s.get(BASE+'/login/',timeout=20).raise_for_status()
    r=s.post(BASE+'/login/',data={'email':email,'password':'LabOpsDemo!2026','csrfmiddlewaretoken':s.cookies['csrftoken']},timeout=20)
    assert r.status_code==200 and '/login' not in r.url,r.text[:500]
    return s
def api(s,route,data=None,key=None,status=None):
    headers={'X-CSRFToken':s.cookies.get('csrftoken',''),'Idempotency-Key':key or str(uuid.uuid4())}
    r=s.request('GET' if data is None else 'POST',BASE+'/api/v1/'+route,json=data,headers=headers,timeout=30)
    assert r.status_code==(status or (200 if data is None else r.status_code)),(route,r.status_code,r.text)
    if status: return r.json()
    assert r.ok,(route,r.status_code,r.text)
    return r.json()['data']
a=login('admin@labops.local'); reviewer=login('reviewer@labops.local'); tech=login('tech@labops.local')
check('anonymous API denied',requests.get(BASE+'/api/v1/me',timeout=10).status_code==401)
check('technician cannot list users',api(tech,'users',status=403)['error']['code']=='FORBIDDEN')
r=a.post(BASE+'/api/v1/items',json={},timeout=10)
check('write requires CSRF',r.status_code==403 and r.json()['error']['code']=='CSRF_FAILED')
for route in ['me','dashboard','system','items','suppliers','warehouses','projects','tasks','purchase-requests','purchase-orders','receipts','inventory','balances','movements','reports','audit','notifications','events','lab-orders','samples','test-catalog','references?kind=items']:
    api(a,route); check('read '+route,True)
item=api(a,'references?kind=items')[0]; supplier=api(a,'references?kind=suppliers')[0]; warehouses=api(a,'references?kind=warehouses')
pr=api(a,'purchase-requests',{'reason':'Cloud runtime purchase','lines':[{'item_id':item['id'],'qty':'10','needed_by':str(date.today()+timedelta(days=7))}]})
pr=api(a,'purchase-requests/'+pr['id']+'/submit',{'expected_version':pr['version']})
check('self approval denied',api(a,'purchase-requests/'+pr['id']+'/decision',{'expected_version':pr['version'],'decision':'APPROVE','reason':'Cloud verification'},status=403)['error']['code']=='SELF_APPROVAL_DENIED')
pr=api(reviewer,'purchase-requests/'+pr['id']+'/decision',{'expected_version':pr['version'],'decision':'APPROVE','reason':'Independent reviewer'})
check('two-person purchase approval',pr['status']=='APPROVED')
po=api(a,'purchase-orders',{'supplier_id':supplier['id'],'lines':[{'request_line_id':pr['lines'][0]['id'],'qty':'10','unit_price':'2.50'}]})
po=api(a,'purchase-orders/'+po['id']+'/confirm',{'expected_version':po['version']})
receipt=api(a,'receipts',{'order_id':po['id'],'lines':[{'order_line_id':po['lines'][0]['id'],'warehouse_id':warehouses[0]['id'],'qty':'10','batch_no':'CLOUD-'+uuid.uuid4().hex[:12],'expires_on':str(date.today()+timedelta(days=365))}]})
key=str(uuid.uuid4()); payload={'expected_version':receipt['version']}; route='receipts/'+receipt['id']+'/post'
posted=api(a,route,payload,key); replay=api(a,route,payload,key)
check('receipt posting idempotent',posted['id']==replay['id'])
receipt=api(a,'receipts/'+receipt['id']); batch=receipt['lines'][0]['batch_id']
check('order fully received',api(a,'purchase-orders/'+po['id'])['status']=='CLOSED')
move=api(a,'stock/transfers',{'batch_id':batch,'from_warehouse_id':warehouses[0]['id'],'to_warehouse_id':warehouses[1]['id'],'qty':'3','reason':'Cloud transfer'})
check('warehouse transfer',move['type']=='TRANSFER')
api(a,'stock/movements/'+move['id']+'/reverse',{'reason':'Cloud reversal'})
check('transfer reversal and conserved inventory',sum(Decimal(x['on_hand_qty']) for x in api(a,'balances?batch_id='+batch))==Decimal('10'))
check('ledger reconciliation',api(a,'stock/reconcile')['differences']==[])
project=next(x for x in api(a,'projects') if x['status']=='ACTIVE'); test=api(a,'test-catalog')[0]
order=api(a,'lab-orders',{'project_id':project['id'],'test_id':test['id']})
sample=api(a,'samples',{'order_id':order['id'],'barcode':'CLOUD-'+uuid.uuid4().hex[:12],'storage_warehouse_id':warehouses[0]['id']})
for state in ['RECEIVED','PROCESSING','COMPLETED']:
    sample=api(a,'samples/'+sample['id']+'/transition',{'expected_version':sample['version'],'target_status':state})
    check('sample '+state,sample['status']==state)
order=api(a,'lab-orders/'+order['id']+'/transition',{'expected_version':order['version'],'target_status':'COMPLETED'})
check('lab order completion',order['status']=='COMPLETED')
check('metrics auth',requests.get(BASE+'/metrics',timeout=10).status_code==403)
r=requests.get(BASE+'/metrics',headers={'Authorization':'Bearer cloud-runtime-metrics'},timeout=10)
check('metrics readable with token',r.ok and 'labops_' in r.text)
Path('output/playwright/persistence.json').write_text(json.dumps({'sample_id':sample['id'],'order_id':order['id'],'batch_id':batch}))
print(json.dumps(results,indent=2))
