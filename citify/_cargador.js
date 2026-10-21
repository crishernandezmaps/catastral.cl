// Cargador de comunas a Citify (se inyecta en app.citify.cl/plataforma/capas). Esperas vía Web Worker: Chrome
// frena los setTimeout de páginas no visibles.
(function(){
if(!window.__wk){const w=new Worker(URL.createObjectURL(new Blob(["onmessage=e=>setTimeout(()=>postMessage(e.data.id),e.data.ms)"],{type:'text/javascript'})));const cb={};let n=0;w.onmessage=e=>{const f=cb[e.data];delete cb[e.data];f&&f()};window.__wk=w;window.__wsleep=ms=>new Promise(r=>{const id=++n;cb[id]=r;w.postMessage({id,ms})})}
const S='http://127.0.0.1:8765/';
// Tooltip acordado con Citify (2026-10-20): solo estos 12 atributos, con nombre legible y formato.
const KEEP={rol:['Rol',null],direccion:['Dirección',null],comuna:['Comuna',null],destino_descripcion:['Destino',null],m2_terreno:['Superficie terreno (m²)','Entero'],sup_construida_total:['Superficie construida (m²)','Entero'],pisos_max:['Pisos','Entero'],anio_construccion_max:['Año de construcción',null],materiales:['Materialidad',null],avaluo_total:['Avalúo fiscal','Moneda'],contrib_sem:['Contribución semestral','Moneda'],val_com_uf:['Valor comercial estimado (UF/m²)','Decimal'],version_datos:['Versión de datos',null]};
window.__atributos=async function(){
 const sleep=window.__wsleep;const D=()=>document.querySelector('[role=dialog]');
 const rows=()=>[...D().querySelectorAll('input[name=attributeSelection]')].map(r=>r.parentElement);
 const key=row=>row.querySelector('p').textContent.trim().replace(/:$/,'');
 let q=0,g=0;while(g++<300){const r=rows().find(x=>!(key(x) in KEEP));if(!r)break;r.querySelector('svg[data-testid=CloseIcon]').closest('button').click();q++;await sleep(100)}
 const set=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set;
 for(const r of rows()){const [alias,fmt]=KEEP[key(r)];const inp=r.querySelector('input[placeholder="Nombre a mostrar"]');
  if(inp.value!==alias){set.call(inp,alias);inp.dispatchEvent(new Event('input',{bubbles:true}));await sleep(120)}
  const cb=r.querySelector('[role=combobox]');if(fmt&&cb.innerText.trim()!==fmt){cb.dispatchEvent(new MouseEvent('mousedown',{bubbles:true}));await sleep(500);const o=[...document.querySelectorAll('[role=option]')].find(x=>x.innerText.trim()==fmt);o&&o.click();await sleep(400)}}
 return {quitados:q,quedan:rows().length};
};

window.__subir=async function(lista){
 const sleep=window.__wsleep;
 const E=window.__est={fase:'inicio',hechos:[],err:null,t0:Date.now()};
 const D=()=>document.querySelector('[role=dialog]');
 const B=t=>D()&&[...D().querySelectorAll('button')].find(b=>b.innerText.split('\n')[0].trim()==t);
 const until=async(f,ms)=>{const t=Date.now();while(Date.now()-t<ms){const v=f();if(v)return v;await sleep(400)}return null};
 const label=n=>[...D().querySelectorAll('*')].find(e=>{if(e.children.length||e.textContent.trim()!=n||e.closest('button'))return false;let c=e.parentElement;for(let i=0;i<3&&c;i++){if(c.querySelectorAll('input[type=file]').length===1)return true;c=c.parentElement}return false});
 try{
  if(!D()||!/Editar capa/.test(D().innerText)) throw 'abre primero el editor';
  E.fase='descargando';
  const files={};
  for(const it of lista){const r=await fetch(S+encodeURIComponent(it.archivo));if(!r.ok)throw 'fetch '+it.archivo+' '+r.status;files[it.archivo]=new File([await r.blob()],it.archivo,{type:'application/geo+json'})}
  if(!await until(()=>!/Cargando datos de la capa/.test(D().innerText)&&(B('Volver a regiones')||D().innerText.includes('Selecciona una regi')),600000)) throw 'editor no cargó';
  await sleep(3000);
  for(const it of lista){
   E.fase='poniendo '+it.comuna;
   let lab=null;
   for(let k=0;k<4&&!lab;k++){
    const v=B('Volver a regiones');if(v){v.click();await sleep(800)}
    const rb=[...D().querySelectorAll('button')].find(b=>b.innerText.split('\n')[0].trim()==it.region);if(!rb)throw 'sin region '+it.region;
    rb.click();lab=await until(()=>label(it.comuna),4000);
   }
   if(!lab)throw 'sin comuna '+it.comuna;
   let c=lab.parentElement,inp;while(c&&!(inp=c.querySelector('input[type=file]')))c=c.parentElement;
   const dt=new DataTransfer();dt.items.add(files[it.archivo]);inp.files=dt.files;inp.dispatchEvent(new Event('change',{bubbles:true}));
   if(!await until(()=>D().innerText.includes(it.archivo),600000)) throw 'no tomó '+it.comuna;
   E.hechos.push(it.comuna);
  }
  const v=B('Volver a regiones');if(v){v.click();await sleep(700)}
  E.conteo=[...D().querySelectorAll('button')].map(b=>b.innerText.replace(/\n/g,' ')).filter(x=>/archivos/.test(x)).join(' | ');
  E.fase='guardando';
  E.atr=await window.__atributos();
  B('Guardar').click();E.clics=1;
  let ok=null;const t1=Date.now();
  while(!ok&&Date.now()-t1<1800000){ok=await until(()=>!D()&&'cerrado',20000);if(!ok&&D()&&B('Guardar')&&!D().querySelector('[role=progressbar]')&&E.clics<8){E.atr2=await window.__atributos();B('Guardar').click();E.clics++}}
  E.fase=ok?'listo':'timeout';E.toast=/Capa actualizada exitosamente/.test(document.body.innerText);
 }catch(e){E.err=String(e);E.fase='error'}
 E.seg=Math.round((Date.now()-E.t0)/1000);
};

window.__reconcilia=async function(){
 const sleep=window.__wsleep;const D=()=>document.querySelector('[role=dialog]');
 if(!D()){const ic=document.querySelector('tr svg[data-testid=EditIcon]');const k=Object.keys(ic).find(k=>k.startsWith('__reactProps'));ic[k].onClick({stopPropagation(){},preventDefault(){},currentTarget:ic,target:ic});}
 for(let w=0;w<60&&!(D()&&!/Cargando datos/.test(D().innerText)&&(D().innerText.includes('Selecciona una regi')||D().innerText.includes('Volver a regiones')));w++) await sleep(1000);
 await sleep(2000);
 const REGS=["Arica y Parinacota","Tarapacá","Antofagasta","Atacama","Coquimbo","Valparaíso","Región Metropolitana","Libertador Gral. Bernardo O’Higgins","Maule","Ñuble","Biobío","Araucanía","Los Ríos","Los Lagos","Aysén del Gral. Carlos Ibáñez del Campo","Magallanes y de la Antártica Chilena"];
 const find=t=>[...D().querySelectorAll('button')].find(x=>x.innerText.split('\n')[0].trim()==t);
 let v=[...D().querySelectorAll('button')].find(x=>/Volver a regiones/.test(x.innerText));if(v){v.click();await sleep(800)}
 const con=[];
 for(const r of REGS){find(r).click();await sleep(800);const t=D().innerText;const j=t.indexOf('Volver a regiones'),k=t.indexOf('Nombres de atributos');const L=t.slice(j,k>j?k:undefined).split('\n').map(s=>s.trim()).filter(Boolean).slice(1);for(let i=0;i<L.length-1;i++){if(/\.geojson$/.test(L[i+1]))con.push(L[i])}v=[...D().querySelectorAll('button')].find(x=>/Volver a regiones/.test(x.innerText));v.click();await sleep(800)}
 const c=[...D().querySelectorAll('button')].find(x=>x.innerText.trim()=='Cancelar');c&&c.click();
 await fetch(S+'_citify_con?'+encodeURIComponent(JSON.stringify(con))).catch(()=>{});
 return con.length;
};
window.__bucle=async function(){
 if(window.__corriendo) return 'ya corre';  // nunca dos ciclos en la misma página
 window.__corriendo=true;
 const sleep=window.__wsleep;
 window.__B=Object.assign(window.__B||{ultimo:null,log:[]},{vivo:true});
 while(window.__B.vivo){
  try{
   while(window.__est&&!/listo|error|timeout/.test(window.__est.fase)) await sleep(5000);
   let t;try{const r=await fetch(S+'_tanda.json?'+Date.now());if(!r.ok)throw 'no';t=await r.json()}catch(_){await sleep(15000);continue}
   if(t.fin){window.__B.log.push('fin');break}
   if(t.id===window.__B.ultimo){await sleep(10000);continue}
   await sleep(2000);
   for(let intento=0;intento<3;intento++){
    let d=document.querySelector('[role=dialog]');
    if(d&&intento>0){const c=[...d.querySelectorAll('button')].find(x=>x.innerText.trim()=='Cancelar');c&&c.click();await sleep(3000);d=null}
    if(!d){const ic=document.querySelector('tr svg[data-testid=EditIcon]');if(ic){const k=Object.keys(ic).find(k=>k.startsWith('__reactProps'));ic[k].onClick({stopPropagation(){},preventDefault(){},currentTarget:ic,target:ic});}}
    for(let w=0;w<60&&!document.querySelector('[role=dialog]');w++) await sleep(1000);
    await sleep(3000);
    await window.__subir(t.lista);
    if(window.__est.fase==='listo') break;
    window.__B.log.push(t.id+':reintento '+(intento+1)+':'+window.__est.err);
    await sleep(10000);
   }
   const E=window.__est;window.__B.ultimo=t.id;
   window.__B.log.push(t.id+':'+E.fase+':'+E.hechos.length+'/'+t.lista.length+':'+E.seg+'s'+(E.err?':'+E.err:''));
   const ok=E.fase==='listo'&&E.hechos.length===t.lista.length;
   await fetch(S+'_fin?'+(ok?'ok':'error')+'='+t.id+'&n='+E.hechos.length).catch(()=>{});
   if(!ok)break;
   window.__B.hechas=(window.__B.hechas||0)+1;
   if(window.__B.hechas>=4){await fetch(S+'_pausa?id='+t.id).catch(()=>{});window.__B.log.push('pausa para recargar');break}
   await sleep(5000);
  }catch(e){window.__B.log.push('exc '+e);await fetch(S+'_fin?error=exc').catch(()=>{});break}
 }
 window.__B.vivo=false;window.__corriendo=false;
};
})();
