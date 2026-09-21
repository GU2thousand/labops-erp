import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
const businessFailures = new Rate('business_failures');
const base=__ENV.BASE_URL || 'http://127.0.0.1:18765';
export const options={vus:Number(__ENV.VUS || 20),duration:__ENV.DURATION || '30s',summaryTrendStats:['avg','p(50)','p(95)','p(99)','max'],thresholds:{http_req_failed:['rate<0.01'],business_failures:['rate<0.01']}};
function login(email) {
  const jar=http.cookieJar();jar.clear(base);
  http.get(base+'/login/');
  let csrf=jar.cookiesForURL(base).csrftoken[0];
  let r=http.post(base+'/login/',{email,password:'BenchmarkLocal!2026',csrfmiddlewaretoken:csrf},{redirects:0,headers:{Referer:base+'/login/'}});
  if(r.status!==302)throw new Error('Login failed '+r.status);
  let cookies=jar.cookiesForURL(base);return {session:cookies.sessionid[0],csrf:cookies.csrftoken[0]};
}
function params(auth,name,key) { return {jar:new http.CookieJar(),cookies:{sessionid:auth.session,csrftoken:auth.csrf},headers:{'X-CSRFToken':auth.csrf,'Content-Type':'application/json','Idempotency-Key':key || 'none'},tags:{name}}; }
function get(path,auth,name) {return http.get(base+'/api/v1/'+path,params(auth,name));}
function write(path,data,auth,name,key) {
  let r=http.post(base+'/api/v1/'+path,JSON.stringify(data),params(auth,name,key));
  businessFailures.add(r.status<200||r.status>=300);
  check(r,{[name+' accepted']:r=>r.status>=200&&r.status<300});
  if(r.status<200||r.status>=300)return null;return r.json().data;
}
export function setup() {
 const admin=login('benchmark@labops.local'),buyer=login('buyer@benchmark.local');
 const data={admin,buyer,items:get('items?page_size=100',admin,'setup items').json().data,balances:get('balances?page_size=100',admin,'setup balances').json().data,orders:get('purchase-orders?page_size=100',admin,'setup orders').json().data,task:get('tasks',admin,'setup tasks').json().data[0]};
 if(!data.task || data.items.length<100 || data.balances.length<100 || data.orders.length<100)throw new Error('Invalid benchmark fixtures or authentication');
 return data;
}
function workload(data) {
 const n=Math.random(),index=(__VU+__ITER)%100,key=`load-${__ENV.RUN_ID || 'run'}-${__VU}-${__ITER}`;
 if(n<.6) {
   let r=get('inventory?page_size=20',data.admin,'GET inventory');businessFailures.add(r.status!==200);check(r,{'inventory ok':r=>r.status===200});
 } else if(n<.85) {
   const item=data.items[index];
   let pr=write('purchase-requests',{reason:'Benchmark request',lines:[{item_id:item.id,qty:1,needed_by:new Date().toISOString().slice(0,10)}]},data.buyer,'POST request',key);
   if(n>=.75 && pr){
     pr=write(`purchase-requests/${pr.id}/submit`,{expected_version:pr.version},data.buyer,'POST submit',key+'-submit');
     if(pr)write(`purchase-requests/${pr.id}/decision`,{expected_version:pr.version,decision:'APPROVE',reason:'Benchmark review'},data.admin,'POST approval',key+'-approve');
   }
 } else if(n<.95) {
   if(Math.random()<.5){
     const b=data.balances[index];
     write('stock/issues',{task_id:data.task.id,lines:[{batch_id:b.batch_id,warehouse_id:b.warehouse_id,qty:'0.001'}]},data.admin,'POST issue',key);
   } else {
     const order=data.orders[index],line=order.lines[0];
     const r=write('receipts',{order_id:order.id,lines:[{order_line_id:line.id,warehouse_id:data.balances[index].warehouse_id,qty:'0.001',batch_no:key}]},data.admin,'POST receipt',key);
     if(r)write(`receipts/${r.id}/post`,{expected_version:r.version},data.admin,'POST receipt posting',key+'-post');
   }
 } else {
   let r=get('reports',data.admin,'GET reports');businessFailures.add(r.status!==200);check(r,{'reports ok':r=>r.status===200});
 }
 sleep(.05);
}

export default function(data) {
 try { workload(data); } catch(error) { businessFailures.add(true);check(false,{'script completed':x=>x});throw error; }
}
