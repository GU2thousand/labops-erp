async (page) => {
  page.setDefaultTimeout(15000);
  const results=[], errors=[], serverErrors=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('response',r=>{if(r.status()>=500)serverErrors.push(r.url()+':'+r.status());});
  const check=async(name,fn)=>{try{await fn();results.push({name,passed:true});}catch(e){results.push({name,passed:false,error:String(e)});}};
  await page.setViewportSize({width:1440,height:1000});
  await check('administrator browser login',async()=>{
    await page.getByLabel('Email',{exact:true}).fill('admin@labops.local');
    await page.getByLabel('Password',{exact:true}).fill('LabOpsDemo!2026');
    await page.getByRole('button',{name:'Sign in'}).click();
    await page.locator('#main h1').waitFor();
    await page.screenshot({path:'output/playwright/lab-desktop.png',fullPage:true});
  });
  console.log(await page.locator('body').ariaSnapshot());
  const links=await page.locator('#nav a').evaluateAll(nodes=>nodes.map(n=>({href:n.getAttribute('href'),name:n.textContent.trim()})));
  if(links.length!==11)results.push({name:'eleven administrator navigation pages',passed:false,error:'actual='+links.length});
  for(const link of links) await check('navigation '+link.name,async()=>{
    const response=page.waitForResponse(r=>r.url().includes('/api/v1/') && r.request().method()==='GET');
    if(('#'+(page.url().split('#')[1]||'dashboard'))===link.href)await page.reload();
    else await page.locator('#nav a').filter({hasText:link.name}).click();
    await (await response).finished();
    await page.waitForFunction(()=>!document.querySelector('#main').textContent.includes('Loading')&&!!document.querySelector('#main h1'));
    if(await page.locator('#main').getByText('Try again',{exact:true}).count())throw Error('Page load failure');
  });
  await check('session survives page reload',async()=>{await page.reload();await page.locator('#main h1').waitFor();});
  await check('mobile navigation and layout',async()=>{
    await page.setViewportSize({width:390,height:844});
    await page.getByRole('button',{name:'Open navigation'}).click();
    await page.locator('#nav a').filter({hasText:'Inventory'}).click();
    await page.locator('#main h1').waitFor();
    await page.screenshot({path:'output/playwright/lab-mobile.png',fullPage:true});
    if(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1))throw Error('Horizontal overflow');
  });
  await page.setViewportSize({width:1440,height:1000});
  await check('browser sign out',async()=>{await page.getByRole('button',{name:'Sign out',exact:true}).click();await page.getByRole('button',{name:'Sign in'}).waitFor();});
  await check('no browser exceptions or HTTP 5xx',async()=>{if(errors.length||serverErrors.length)throw Error(JSON.stringify({errors,serverErrors}));});
  return results;
}
