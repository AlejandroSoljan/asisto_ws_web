/*script:app_asisto*/
/*version:1.06.02   11/07/2025*/
// ──────────────────────────────────────────────────────────────────────────────
// MULTI-SESIÓN + PÁGINAS QR POR CUENTA
// - Principal: sirve /qr/:id para ver cada QR.
// - Workers: reenvían io.emit(...) por IPC al principal.
// Uso:
//   WA_CLIENT_IDS="cliente1,cliente2"   (el principal usa la 1ª; los demás se forkean)
//   /qr/cliente1  /qr/cliente2  → para escanear cada QR en páginas separadas.
// ──────────────────────────────────────────────────────────────────────────────
const { fork } = require('child_process');
const IS_WORKER = process.env.CHILD_WORKER === '1';
function spawnAdditionalClientsIfNeeded() {
  if (IS_WORKER) return;
  const list = (process.env.WA_CLIENT_IDS || process.env.WA_CLIENT_ID || 'default')
    .split(',').map(s => s.trim()).filter(Boolean);
  if (list.length <= 1) return;
  // === LIMITE DE SESIONES Y SPAWN ESCALONADO PARA BAJA RAM ===
  const MAX = Math.max(1, parseInt(process.env.MAX_WA_CLIENTS || '1', 10)); // por defecto 1
  const DELAY = Math.max(0, parseInt(process.env.WORKER_SPAWN_DELAY_MS || '20000', 10)); // 20s
  // El principal conserva el primero y levanta HTTP/Socket.IO.
  const idsToSpawn = list.slice(1, 1 + (MAX - 1));
  idsToSpawn.forEach((id, idx) => {
    setTimeout(() => {
      const child = fork(process.argv[1], [], {
        env: {
          ...process.env,
          WA_CLIENT_ID: id,
          CHILD_WORKER: '1',
          SKIP_HTTP_SERVER: '1',
          // Limita heap de Node en workers para no exceder 512MB totales
          NODE_OPTIONS: process.env.NODE_OPTIONS || '--max-old-space-size=256'
        }
      });
      child.on('message', (msg) => { try { global.__io && global.__io.emit(msg.type, msg.payload); } catch(_) {} });
      console.log('[MASTER] Spawned worker for', id);
    }, DELAY * (idx + 1));
  });
}
spawnAdditionalClientsIfNeeded();
// ====== VARIABLES DE ENTORNO (deben declararse ANTES de requerir puppeteer) ======
process.env.PUPPETEER_PRODUCT = process.env.PUPPETEER_PRODUCT || 'chrome';
// Cache/carpeta de descarga en el Disk persistente:
process.env.PUPPETEER_CACHE_DIR = process.env.PUPPETEER_CACHE_DIR || '/var/data/.cache/puppeteer';
process.env.PUPPETEER_DOWNLOAD_PATH = process.env.PUPPETEER_DOWNLOAD_PATH || '/var/data/.cache/puppeteer';

// ====== IMPORTS ======
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const puppeteer = require('puppeteer');
const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');
const os = require('os');
const express = require('express');
const { body, validationResult } = require('express-validator');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
const fetch = require('node-fetch');
const fileUpload = require('express-fileupload');
const axios = require('axios');
const mime = require('mime-types');
const { ClientInfo } = require('whatsapp-web.js/src/structures');
const utf8 = require('utf8');
const nodemailer = require('nodemailer');
const { eventNames } = require('process');

// === Persistencia de sesión en Render ===
// Usa EXACTAMENTE el mount path del Disk. Por defecto Render recomienda /var/data.
// Configura en el Dashboard un Disk con Mount Path /var/data.
const WA_CLIENT_ID = process.env.WA_CLIENT_ID || 'default';
const SESSIONS_DIR = process.env.SESSIONS_DIR || '/var/data/wwebjs';



// Verifica que el directorio exista y sea escribible (si falla, corta el proceso).
function ensureWritableDir(dir) {
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.accessSync(dir, fs.constants.W_OK);
    console.log('[BOOT] Usando dataPath para LocalAuth →', dir);
  } catch (err) {
    console.error('[BOOT] No puedo escribir en', dir, '→', err.message);
    console.error('[BOOT] Verificá en Render > Disks que el Mount Path sea /var/data (o ajustá SESSIONS_DIR).');
    process.exit(1);
  }
}
ensureWritableDir(SESSIONS_DIR);

// === CONFIG PERSISTENTE EN DISK ===
const CONFIG_DIR = process.env.CONFIG_DIR || path.join('/var/data', 'config');
ensureWritableDir(CONFIG_DIR);
const CONFIG_MAIN = path.join(CONFIG_DIR, 'configuracion.json');
const CONFIG_ERRS = path.join(CONFIG_DIR, 'configuracion_errores.json');

// Copia inicial desde el repo a /var/data/config si no existen
function seedConfigIfMissing() {
  try {
    const repoMain = path.join(__dirname, 'configuracion.json');
    const repoErrs = path.join(__dirname, 'configuracion_errores.json');
    if (!fs.existsSync(CONFIG_MAIN) && fs.existsSync(repoMain)) {
      fs.copyFileSync(repoMain, CONFIG_MAIN);
    }
    if (!fs.existsSync(CONFIG_ERRS) && fs.existsSync(repoErrs)) {
      fs.copyFileSync(repoErrs, CONFIG_ERRS);
    }
  } catch(e) {
    console.error('[CONFIG] Error al inicializar config persistente:', e.message);
  }
}
seedConfigIfMissing();


ensureWritableDir(process.env.PUPPETEER_CACHE_DIR);

// ========= RESOLVER/INSTALAR CHROME EN RUNTIME (fallback seguro) =========
function fileExists(p) {
  try { return p && fs.existsSync(p); } catch { return false; }
}

function tryPuppeteerExecutablePath() {
  try {
    const p = puppeteer.executablePath();
    return fileExists(p) ? p : null;
  } catch { return null; }
}

function tryCommonSystemPaths() {
  const candidates = [
    process.env.CHROME_PATH,
    process.env.PUPPETEER_EXECUTABLE_PATH,
    '/usr/bin/google-chrome',
    '/usr/bin/chromium-browser',
    '/usr/bin/chromium',
  ].filter(Boolean);
  for (const c of candidates) if (fileExists(c)) return c;
  return null;
}

function installChromeIfNeeded() {
  // Descarga Chrome/Chromium dentro de /var/data/.cache/puppeteer
  // Nota: requiere que 'puppeteer' esté en dependencias.
  try {
    console.log('[CHROME] Instalando Chrome con puppeteer (una sola vez)…');
    execSync('npx puppeteer browsers install chrome', {
      stdio: 'inherit',
      env: { ...process.env, PUPPETEER_CACHE_DIR: process.env.PUPPETEER_CACHE_DIR }
    });
  } catch (e) {
    console.error('[CHROME] Falló la instalación automática:', e?.message || e);
  }
}

// Devuelve el binario instalado bajo el caché persistente (/var/data/.cache/puppeteer)
function resolveInstalledChromePath() {
  try {
    const base = process.env.PUPPETEER_CACHE_DIR || '/var/data/.cache/puppeteer';
    const root = path.join(base, 'chrome');
    const versions = fs.readdirSync(root).filter(Boolean);
    for (const v of versions) {
      const candidate = path.join(root, v, 'chrome-linux64', 'chrome');
      if (fs.existsSync(candidate)) return candidate;
    }
  } catch (_) {}
  return null;
}



function getChromePathOrInstall() {
  // 1) env / system
  let found = tryCommonSystemPaths() || tryPuppeteerExecutablePath();
  if (found) return found;
  // 2) instalar y reintentar
  installChromeIfNeeded();
   // Priorizar el binario instalado en el caché persistente
  found = resolveInstalledChromePath() || tryPuppeteerExecutablePath();
  if (found) {
    // Exportar también para que puppeteer-core lo tome
    process.env.PUPPETEER_EXECUTABLE_PATH = found;
    console.log('[CHROME] Usando ejecutable instalado →', found);
    return found;
  }
  console.warn('[CHROME] No se encontró Chrome tras instalación; Puppeteer intentará su resolución interna.');
  return undefined;
}


///////////////////
/*function getChromePathOrInstall() {
  try {
    const p = puppeteer.executablePath();
    if (p && fs.existsSync(p)) return p;
  } catch (e) {
    console.error('puppeteer.executablePath() falló:', e && e.message ? e.message : e);
  }
  try {
    console.log('Instalando Chrome (runtime)...');
    execSync('npx puppeteer browsers install chrome', { stdio: 'inherit' });
    const p2 = puppeteer.executablePath();
    if (p2 && fs.existsSync(p2)) return p2;
  } catch (e) {
    console.error('Instalación runtime de Chrome falló:', e && e.message ? e.message : e);
  }
  return null;
}*/




var a = 0;
//var port = 8002
var headless = true;
var seg_desde = 80000;
var seg_hasta = 10000;
var seg_msg = 5000;
var seg_tele = 3000;
var version = "1.0";
var script = "__";
var telefono_qr = "0";
var telefono_local = "0";
var tel_array = [];
var ver_whatsapp = "0";
var dsn = "msm_manager";
var api = "http://managermsm.ddns.net:2002/v200/api/Api_Chat_Cab/ProcesarMensajePost";
//var api2 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta?key=1";
var api2 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta_no_enviados"//key={{key}}&nro_tel_from={{nro_tel_from}}";
var api3 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Actualiza_mensaje"//?key={{key}}&nro_tel_from={{nro_tel_from}}"//key={{key}}&nro_tel_from={{nro_tel_from}}";
var key = 'FMM0325*';
var msg_inicio = "";
var msg_fin = "";
var cant_lim = 0;
var msg_lim = 'Continuar? S / N';
var time_cad = 0;
var email_err = "";
var msg_cad = "";
var msg_can = "";
var bandera_msg = 1;
var jsonGlobal = [];   //1-json, 2 -i , 3-tel, 4-hora
var json;
var i_global = 0;
var msg_body;
var smtp;
var email_usuario;
var email_pas;
var email_puerto;
var email_saliente;
var msg_errores;
var nom_chatbot;

var Id_msj_dest ;
var Id_msj_renglon;
const controller = new AbortController();
const timeoutId = setTimeout(() => controller.abort(), 55000);

var signatures = {
  JVBERi0: "application/pdf",
  R0lGODdh: "image/gif",
  R0lGODlh: "image/gif",
  iVBORw0KGgo: "image/png",
  "/9j/": "image/jpg"
};



const logFilePath_event = path.join(__dirname, 'app_asisto_event.log');
const logFilePath_error = path.join(__dirname, 'app_asisto_error.log');

EscribirLog("inicio Script","event");


const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = socketIO(server);
global.__io = io; // para emitir desde manejadores del proceso principal y desde IPC

// Si es WORKER, no hay clientes Socket.IO conectados; redirigimos io.emit → IPC
if (process.env.SKIP_HTTP_SERVER === '1' && typeof process.send === 'function') {
  const _emit = io.emit.bind(io);
  io.emit = (type, payload) => {
    try { process.send({ type, payload }); } catch(_) {}
    return _emit(type, payload);
  };
}

app.use(express.json());
app.use(express.urlencoded({
  extended: true
}));

// === AUTH ADMIN SIMPLE ===
const ADMIN_TOKEN = process.env.ADMIN_TOKEN;
function requireAdmin(req, res, next) {
  const header = req.headers['x-admin-token'] || req.query.token;
  if (!ADMIN_TOKEN) return res.status(500).json({ error: 'ADMIN_TOKEN no configurado' });
  if (header !== ADMIN_TOKEN) return res.status(401).json({ error: 'No autorizado' });
  next();
}

// === API de configuración ===
app.get('/api/config', requireAdmin, (req, res) => {
  try {
    const main = JSON.parse(fs.readFileSync(CONFIG_MAIN, 'utf-8'));
    const errs = JSON.parse(fs.readFileSync(CONFIG_ERRS, 'utf-8'));
    res.json({ main, errs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/config', requireAdmin, express.json({ limit: '1mb' }), (req, res) => {
  try {
    const { main, errs } = req.body || {};
    if (!main || !errs) return res.status(400).json({ error: 'Body debe incluir { main, errs }' });
    fs.writeFileSync(CONFIG_MAIN, JSON.stringify(main, null, 2));
    fs.writeFileSync(CONFIG_ERRS, JSON.stringify(errs, null, 2));
    // Opcional: refrescar variables en caliente
    try { RecuperarJsonConf(); } catch(_) {}
    try { RecuperarJsonConfMensajes(); } catch(_) {}
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// === Página simple de administración ===
app.get('/admin', (req, res) => {
  res.send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Admin Config</title>
  <style>body{font-family:system-ui,Segoe UI,Roboto,sans-serif;margin:24px}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  textarea{width:100%;height:60vh;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px}
  .bar{display:flex;gap:8px;align-items:center;margin:8px 0}.ok{color:green}.err{color:#b00020}.hint{color:#666;font-size:12px}</style>
  </head><body>
  <h2>Panel de Configuración</h2>
  <div class="bar">
    <input id="token" type="password" placeholder="ADMIN_TOKEN">
    <button id="load">Cargar</button>
    <button id="save">Guardar</button>
    <span id="status" class="hint"></span>
  </div>
  <div class="row">
    <div><h3>configuracion.json</h3><textarea id="main"></textarea></div>
    <div><h3>configuracion_errores.json</h3><textarea id="errs"></textarea></div>
  </div>
  <script>
    const $=id=>document.getElementById(id);
    const status=(m,c='hint')=>{const s=$('status');s.textContent=m;s.className=c;};
    document.getElementById('load').onclick=async()=>{
      status('Cargando…');
      try{
        const r=await fetch('/api/config',{headers:{'x-admin-token':$('token').value}});
        const j=await r.json(); if(!r.ok) throw new Error(j.error||'Error');
        $('main').value=JSON.stringify(j.main,null,2);
        $('errs').value=JSON.stringify(j.errs,null,2);
        status('Listo ✔','ok');
      }catch(e){ status(e.message,'err'); }
    };
    document.getElementById('save').onclick=async()=>{
      status('Guardando…');
      try{
        const main=JSON.parse($('main').value);
        const errs=JSON.parse($('errs').value);
        const r=await fetch('/api/config',{method:'PUT',headers:{'content-type':'application/json','x-admin-token':$('token').value},body:JSON.stringify({main,errs})});
        const j=await r.json(); if(!r.ok) throw new Error(j.error||'Error');
        status('Guardado ✔','ok');
      }catch(e){ status(e.message,'err'); }
    };
  </script></body></html>`);
});



// Página simple para ver el QR de un clientId específico
app.get('/qr/:id', (req, res) => {
  const id = req.params.id;
  res.send(`<!doctype html><html><head><meta charset="utf-8"><title>QR ${id}</title></head>
  <body style="font-family:system-ui;margin:20px">
    <h2>QR de <code>${id}</code></h2>
    <p>Esperando QR… si ya está autenticado, no aparecerá nada.</p>
    <div id="box" style="border:1px solid #ddd;padding:16px;display:inline-block">
      <img id="qr" style="max-width:320px;display:none" />
    </div>
    <pre id="log" style="background:#f7f7f7;border:1px solid #eee;padding:8px;max-width:640px;overflow:auto"></pre>
    <script src="/socket.io/socket.io.js"></script>
    <script>
      const id = ${JSON.stringify(id)};
      const log = (m)=>{ document.getElementById('log').textContent += m+"\\n"; };
      const img = document.getElementById('qr');
      const socket = io();
      socket.on('connect', ()=> log('socket conectado'));
      socket.on('qr', (data)=>{
        if(!data || data.id !== id) return;
        img.src = data.url; img.style.display='block'; log('QR actualizado');
      });
      socket.on('message', (m)=>{ if((m||'').includes('Código QR') && (m||'').includes(id)) log(m); });
      socket.on('authenticated', (m)=>{ if((m||'').includes(id)) log(m); });
    </script>
  </body></html>`);
});


app.use(fileUpload({
  debug: false
}));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

RecuperarJsonConf();
//headless = devolver_headless();

//const port =  devolver_puerto();


// Solo el proceso principal abre el puerto HTTP
if (process.env.SKIP_HTTP_SERVER !== '1') {
  server.listen(process.env.PORT || port, function() {
    console.log('App running on *: ' + port, '| host:', os.hostname());
    EscribirLog('App running on *: ' + port,"event");
  });
} else {
  console.log('[WORKER] Proceso sin HTTP server | clientId =', process.env.WA_CLIENT_ID);
}

//try { fs.mkdirSync(path.join(__dirname, 'sessions'), { recursive: true }); } catch (e) { console.error('No se pudo crear carpeta de sesión:', e?.message || e); }
const client = new Client({
  restartOnAuthFail: true,

  // 👇 USAR SIEMPRE LA VERSIÓN LIVE DE WA WEB
  // (si hay loop de QR con una versión fijada, esto lo corta)
  webVersionCache: { type: 'none' },

  puppeteer: {
    // respetar tu config.json
    headless: true,
    // No cortes nunca operaciones del protocolo (navegaciones largas/recargas)
    protocolTimeout: 0,
    waitForInitialPage: true,
    executablePath: (getChromePathOrInstall() || process.env.PUPPETEER_EXECUTABLE_PATH || puppeteer.executablePath()),
   
    // Flags estables + "LOW-RAM" para 512MB
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--no-zygote',
      '--single-process',                 // ↓ RAM (si vieras inestabilidad, quitarlo)
      '--renderer-process-limit=1',       // fuerza 1 renderer
      '--disable-gpu',
      '--disable-extensions',
      '--disable-features=TranslateUI,BlinkGenPropertyTrees',
      '--media-cache-size=0',
      '--disk-cache-size=0',
      '--mute-audio',
      '--window-size=1280,800',           // ventana más chica = menos consumo
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--no-first-run',
      '--no-default-browser-check'
    ]
  },

  authStrategy: new LocalAuth({
    clientId: WA_CLIENT_ID,
    dataPath: SESSIONS_DIR // 👈 en el Disk persistente (/var/data/wwebjs)
  })
});
// ====== LOGS DE ESTADO PARA DIAGNÓSTICO FINO ======
try {
  client.on('loading_screen', (percent, message) => {
    console.log(`[LOAD] ${percent}% - ${message || ''}`);
  });
  client.on('change_state', (state) => {
    console.log('[STATE]', state);
  });
  client.on('ready_state', (state) => {
    console.log('[READY_STATE]', state);
  });
} catch(_) {}

// ====== EVITAR QUE SE PISEN QRs MIENTRAS EMPAREJÁS ======
// Si no hay sesión activa aún, y tenés WA_CLIENT_IDS definidos,
// desactiva el spawn de workers hasta que este proceso quede autenticado.
let __pairingDone = false;
client.on('authenticated', () => { __pairingDone = true; });
client.on('qr', () => {
  if (process.env.CHILD_WORKER !== '1' && !__pairingDone) {
    // Señal mínima: durante pairing, no crear nuevos workers
    // (en tu código el spawn ocurre al inicio; si usás varios IDs, seteá MAX_WA_CLIENTS=1)
    process.env.MAX_WA_CLIENTS = '1';
  }
});




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
async function ConsultaApiMensajes(){
  console.log("Consultando a API ");
  let url =  api2+'?key='+key+'&nro_tel_from='+telefono_qr;
  let url_confirma_msg = api3+'?key='+key+'&nro_tel_from='+telefono_qr;
  var a= 0;
  console.log("Conectando a API "+url);
  EscribirLog("Conectando a API "+url,"event");




 // await io.emit('message', "Conectando a API "+url );
  
  await sleep(1000);

     while(a < 10){

      RecuperarJsonConfMensajes();
   
     seg_msg = Math.random() * (devolver_seg_hasta() - devolver_seg_desde()) + devolver_seg_desde();
      
     //console.log('segundos entre mensajes '+seg_msg);
         try{
     

        resp = await fetch(url, {
            method: "GET"
        })

        .catch(err =>   EscribirLog(err,"error"))
      //  console.log('resp '+JSON.stringify(resp));
      //  await io.emit('message', JSON.stringify(resp) );
        if (resp.ok  ) {
     
          tam_json = 0;
          json = await resp.json()
  
          var tam_mensajes = Object.keys(json[0].mensajes).length

          var tam_destinatarios = Object.keys(json[0].destinatarios).length
       
          for(var i = 0; i < tam_destinatarios; i++){


            //filtro mensaje en json
            function obtenerMensajesPorId() {
              return Id_msj_renglon = json[0].destinatarios[i].Id_msj_renglon;
            }
            function buscarID(element) {
                return element.Id_msj_renglon == obtenerMensajesPorId();
            }

           // obtengo json con mensaje filtrado
            var respuesta = json[0].mensajes.filter(buscarID)


            var tam_mensajes = Object.keys(respuesta).length
           // console.log("tamaño j "+tam_mensajes)

            for(var j = 0; j < tam_mensajes; j++){
              console.log('--------------------------------------------------')
               Id_msj_dest = json[0].destinatarios[i].Id_msj_dest;
               Id_msj_renglon = json[0].destinatarios[i].Id_msj_renglon;
              var Nro_tel = json[0].destinatarios[i].Nro_tel;
              var Nro_tel_format = Nro_tel+'@c.us';
              var Fecha_envio = respuesta[j].Fecha_envio;
              var Nro_tel_from = respuesta[j].Nro_tel_from;
              var nro_tel_from_format = Nro_tel_from+'@c.us';
              var Msj = respuesta[j].Msj;
              var contenido = respuesta[j].Content;
              var Content_nombre = respuesta[j].Content_nombre;
              var Id_msj_renglon = respuesta[j].Id_msj_renglon;

              console.log("Id_msj_dest "+JSON.stringify(json[0].destinatarios[i].Id_msj_dest))
              console.log("Id_msj_renglon "+JSON.stringify(json[0].destinatarios[i].Id_msj_renglon))
              console.log("Nro_tel "+JSON.stringify(json[0].destinatarios[i].Nro_tel))
              console.log("Fecha_envio "+JSON.stringify(respuesta[j].Fecha_envio))
              console.log("Nro_tel_from "+JSON.stringify(respuesta[j].Nro_tel_from))
              console.log("Msj "+JSON.stringify(respuesta[j].Msj))
              console.log("Content_nombre "+JSON.stringify(respuesta[j].Content_nombre))
              console.log("Id_msj_renglon "+JSON.stringify(respuesta[j].Id_msj_renglon))
              console.log('--------------------------------------------------')
              //console.log('IS REGISTER '+Nro_tel)  ;
              if (!isNaN(Number(Nro_tel))) {
              if (!await client.isRegisteredUser(Nro_tel_format)) {
                 EscribirLog('Mensaje: '+Nro_tel_format+': Número no Registrado',"event");
                console.log("numero no registrado");
                await io.emit('message', 'Mensaje: '+Nro_tel_format+': Número no Registrado' );
                actualizar_estado_mensaje(url_confirma_msg,'I',null,null,null,null,null,json[0].destinatarios[i].Id_msj_renglon,json[0].destinatarios[i].Id_msj_dest );
                   
              
              }
              else { 
               
              if(Content_nombre == null || Content_nombre == ''){Content_nombre = 'archivo'}
               
                //if (contenido != null || contenido != '' ){
                 if (contenido != null ){
                  console.log('tipo de dato: '+detectMimeType(contenido));
                  var media = await new MessageMedia(detectMimeType(contenido), contenido, Content_nombre);
                  //await client.sendMessage(Nro_tel_format,media,{caption:  Msj });
                  await io.emit('message', 'Mensaje: '+Nro_tel_format+': '+ Msj );
                  await client.sendMessage('5493462674128@c.us',media,{caption:  Msj });
                  
                  } else{
                    console.log("msj texto");
                    if (Msj == null){ Msj = ''}
                    //await client.sendMessage(Nro_tel_format,  Msj );
                    await io.emit('message', 'Mensaje: '+Nro_tel_format+': '+ Msj );
                    await client.sendMessage('5493462674128@c.us',  Msj );
                  }
                  /////////////////////////////////////////
                  let contact = await client.getContactById(Nro_tel_format);
                      let tipo, contacto, email, direccion, nombre

                    if(contact.isBusiness==true){
                        tipo = 'B';
                        contacto = contact.pushname;
                        email= contact.businessProfile.email;
                        direccion = contact.businessProfile.address;
                        nombre = contact.name;
                      } else{
                        tipo = 'C';
                        nombre =  contact.name;
                        contacto = contact.shortName;
                        direccion = null;
                        email = null;
                    }
                    actualizar_estado_mensaje(url_confirma_msg,'E',tipo,nombre,contacto,direccion,email,json[0].destinatarios[i].Id_msj_renglon,json[0].destinatarios[i].Id_msj_dest );
                  
                    await sleep(seg_msg);
                  
                }
              }
              else
              {
                console.log("numero invalido");
                await io.emit('message', 'Mensaje: '+Nro_tel_format+': Número Inválido' );
                actualizar_estado_mensaje(url_confirma_msg,'I',null,null,null,null,null,json[0].destinatarios[i].Id_msj_renglon,json[0].destinatarios[i].Id_msj_dest );
               

              }


                
          }   console.log('--------------------------------------------------')
          
        }
      }
     else
     {
      json = await resp.json();
       
       if (msg_errores == ''|| msg_errores == null){

       }
       else{
        //console.log("ApiWhatsapp - Response ERROR "+JSON.stringify(json));
       // EscribirLog("ApiWhatsapp - Response ERROR "+JSON.stringify(json),"error");
       
       }
      // return 'error'
     }
 }
 catch (err) {
   console.log(err);
    EscribirLog(err,"error");
   if (msg_errores == ''|| msg_errores == null){
   }
   else{
    
   
    
   }
   //return 'error'
 }
  RecuperarJsonConfMensajes();
  seg_tele = devolver_seg_tele();
  //console.log("esperando por API "+seg_tele/1000 + ' segundos...');
  await sleep(seg_tele);
    }

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

client.on('message', async message => {

  var indice_telefono = indexOf2d(message.from);

 if(indice_telefono == -1){

  var valor_i=0;
 }else{
 var valor_i = jsonGlobal[indice_telefono][1];
 }
 
EscribirLog(message.from +' '+message.to+' '+message.type+' '+message.body ,"event");


  console.log("mensaje "+message.from+': '+ message.body );

//client.sendMessage('5493462674128@c.us',message.body  );
 await io.emit('message', 'Mensaje: '+message.from+': '+ message.body );


if(message.from == 'status@broadcast'){
      console.log("mensaje de estado");
      return
    }
    if(message.type !== 'chat'){
      console.log("mensaje <> texto");
      return
    }

    if(message.to == ''|| message.to == null){
      console.log("message.to VACIO");
      return
    }

    if(message.from == ''|| message.from == null){
      console.log("message.from VACIO");
      return
    }



 await safeSendMessage(client, '5493462674128@c.us', message.body    );
 /*
  
if (message.from=='5493462514448@c.us'   ){

  
    
  if( valor_i==0) {
    
    RecuperarJsonConfMensajes();
   
    var segundos = Math.random() * (devolver_seg_hasta - devolver_seg_desde) + devolver_seg_desde;

   
   var telefonoTo = message.to;
   var telefonoFrom = message.from;
    //var telefonoFrom = '5493425472992@c.us' 
   // var telefonoTo = '5493424293943@c.us'

    telefonoTo = telefonoTo.replace('@c.us','');
    telefonoFrom = telefonoFrom.replace('@c.us','');
   
    var resp = null;
 

    if(telefonoFrom == 'status@broadcast'){
      console.log("mensaje de estado");
      return
    }
    if(message.type !== 'chat'){
      console.log("mensaje <> texto");
      return
    }

    if(message.to == ''|| message.to == null){
      console.log("message.to VACIO");
      return
    }

    if(message.from == ''|| message.from == null){
      console.log("message.from VACIO");
      return
    }
    console.log("mensaje");
   
      //////////////////////////////////////////////////////////
      // MENSAJE DE ESPERO POR FAVOR
      ////////////////////////////////////////////////////////
      if (msg_inicio == ''|| msg_inicio == null){
      }
      else{
        client.sendMessage(message.from,msg_inicio );
      }

      await io.emit('message', 'Mensaje: '+message.from+': '+ message.body );

      var jsonTexto = {
  Tel_Origen: telefonoFrom ?? "",
  Tel_Destino: telefonoTo ?? "",
  Mensaje: message?.body ?? "",
  Respuesta: ""
};
   
      jsonTexto = {Tel_Origen:telefonoFrom,Tel_Destino:telefonoTo, Mensaje:message.body,Respuesta:''};
      //jsonTexto = {Tel_Origen:'5493462674128',Tel_Destino:'5493424293943', Mensaje:message.body};
     //  jsonTexto = {Tel_Origen:'5493425472992',Tel_Destino:'5493424213214', Mensaje:message.body};

      let url =  api

      console.log(JSON.stringify(jsonTexto));
      EscribirLog("Mensaje "+JSON.stringify(jsonTexto),'event');
      
   try{
         
 
         const resp = await fetch(url, {
           method: "POST",
           body: JSON.stringify(jsonTexto),
           headers: {"Content-type": "application/json; charset=UTF-8"},
        signal: controller.signal
         });

           clearTimeout(timeoutId);
        
         const raw = await resp.text();
        let json = null;
        try { json = raw ? JSON.parse(raw) : null; } catch {  }

         //json = await resp.json();
         console.log(json)
         
         if (!resp.ok) {
            const detalle = json ? JSON.stringify(json) : raw;
             EscribirLog("Error 02 ApiWhatsapp - Response ERROR " + detalle, "error");
             EnviarEmail("ApiWhatsapp - Response ERROR ", detalle);
             if (msg_errores) await client.sendMessage(message.from, msg_errores);
            return "error";
          }

      

          tam_json = 0; // 👈 evitá globals sin const/let; ideal: const tam_json = 0;
            recuperar_json(message.from, json);
            await procesar_mensaje(json, message);

            if (msg_fin) {
               await client.sendMessage(message.from, msg_fin);
            }



           return "ok";

} catch (err) {
  //clearTimeout(timeoutId);
  // Importante: no dependas de jsonTexto indefinido en el log
  const detalle = "Error 03 Chatbot Error " + (err?.message || err) + " " + JSON.stringify(jsonTexto);
 console.log(detalle);
  ////EscribirLog(detalle, "error");
  //EnviarEmail("Chatbot Error ", detalle);
//  if (msg_errores) await client.sendMessage(message.from, msg_errores);
//  return "error";
}





////////////////////
    };
   
    var body = message.body;
    body = body.trim();
    body = body.toUpperCase();


    if(valor_i !== 0 && body == 'N' ){
      console.log("cancelar"&msg_can);
      //client.sendMessage(message.from,'*Consulta Cancelada* ❌' );
            
      if(msg_can == '' || msg_can == undefined || msg_can == 0){
        
        
      }else{
        client.sendMessage(message.from,msg_can );

      }
      bandera_msg=1;
      jsonGlobal[indice_telefono][2] = '';
      jsonGlobal[indice_telefono][1] = 0;
      jsonGlobal[indice_telefono][3] = '';

    
    };
    if(valor_i!==0 && ((body != 'N') && (body != 'S' ) )){
      console.log("no entiendo ->"+message.body);
      client.sendMessage(message.from,'🤔 *No entiendo*, \nPor favor ingrese *S* o *N* para mostrar los siguientes resultados\n ' );

    };
    

    if(valor_i !== 0 && body == 'S'){
      console.log("continuar "+tam_json+' indice '+indice_telefono);
      procesar_mensaje(jsonGlobal[indice_telefono][2], message);

     }
 }  //
*/
});






/*client.on('message_ack', (message2, ack) => {


  console.log('Mensaje ' + message2.id.id);
  console.log('Estado ' + ack);
  console.log('id_msg '+Id_msj_dest+'  id_renlon '+Id_msj_renglon);
});
*/

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
client.initialize();




client.on('ready', async () => {
  console.log("listo ready....");
  telefono_qr = client.info.me.user
  console.log("TEL QR: "+client.info.me.user);
  
    
   await io.emit('message', 'Whatsapp Listo!');
   EscribirLog('Whatsapp Listo!',"event");

  ConsultaApiMensajes();


});

client.on('qr', (qr) => {
  console.log('QR RECEIVED', qr);
  qrcode.toDataURL(qr, (err, url) => {
    // Emitimos incluyendo el clientId actual para distinguir páginas
    const payload = { id: (process.env.WA_CLIENT_ID || 'default'), url };
    io.emit('qr', payload);
    io.emit('message', `[${process.env.WA_CLIENT_ID || 'default'}] Código QR Recibido, por favor escanea`);

  });
});


client.on('authenticated', async () => {
  io.emit('authenticated', `[${process.env.WA_CLIENT_ID || 'default'}] Whatsapp Autenticado!.`);
  io.emit('message', `[${process.env.WA_CLIENT_ID || 'default'}] Whatsapp Autenticado!`);
 console.log('Autenticado');
  EscribirLog('Autenticado',"event");
});



client.on('auth_failure', function(session) {
  io.emit('message', 'Auth failure, restarting...');
  EnviarEmail('Chatbot error Auth failure','Auth failure, restarting...'+client);
  EscribirLog('Error 04 - Chatbot error Auth failure','Auth failure, restarting...',"error");
});

client.on('disconnected', (reason) => {
  io.emit('message', 'Whatsapp Desconectado!');
  EnviarEmail('Chatbot Desconectado ','Desconectando...'+client);
  EscribirLog('Chatbot Desconectado ','Desconectando...',"event");// client.initialize() duplicado eliminado

  client.initialize();
});




function recuperar_json(a_telefono, json){

  var indice =indexOf2d(a_telefono);


  let now = new Date();
 
  if(indice !== -1){
   // console.log("ESTA "+a_telefono);
   
    jsonGlobal[indice][0] = a_telefono;
   // jsonGlobal[a_telefono,2] = 0;
    jsonGlobal[indice][2] = json;
    jsonGlobal[indice][3] = now;
    //console.table(jsonGlobal);
 }else{

    //console.log("NO ESTA "  +a_telefono);
     
  jsonGlobal.push([a_telefono,0,json,now])
    
      
 }


}

function indexOf2d(itemtofind) {
  var valor = -1
  console.table(jsonGlobal);

  for (var i = 0; i < jsonGlobal.length; i++) {
    
    if(jsonGlobal[i][0]==itemtofind){
      console.log('array '+jsonGlobal[i][0]);
      return i
    } else{

      valor = -1
    }
  }

  return valor


  //console.log('indice_a '+[].concat.apply([], ([].concat.apply([], myArray))).indexOf(itemtofind));
  //console.log('indice_b '+myArray.indexOf(itemtofind));
  //console.log('indice_c '+myArray(0).indexOf(itemtofind));
  //return [].concat.apply([], ([].concat.apply([], myArray))).indexOf(itemtofind) !== -1;
  //return [].concat.apply([], ([].concat.apply([], myArray))).indexOf(itemtofind) ;
 
  }

/////////////////////////////////////////////////////////////////////////////////////
// FUNCION DONDE SE PROCESA EL JSON GLOBAL DE MSG Y SE ENVIA
////////////////////////////////////////////////////////////////////////////////

async function procesar_mensaje(json, message){
  
  RecuperarJsonConfMensajes();

  var indice =indexOf2d(message.from);
  let now = new Date();

  var segundos = Math.random() * (seg_hasta - seg_desde) + seg_desde;
  var l_from = message.from;
  var l_json =jsonGlobal[indice][2];
  var l_i = jsonGlobal[indice][1];
  var tam_json =0;
  
  jsonGlobal[indice][3] = now;
  

  console.table(jsonGlobal);
 
  for(var j in jsonGlobal[indice][2]){
    tam_json = tam_json + 1;
  }

  for( var i=jsonGlobal[indice][1]; i < tam_json; i++){
   
    if(l_json[i].cod_error){ 
      var mensaje = l_json[i].msj_error;
      EscribirLog('Error 05 - procesar_mensaje() devuelve cod_error API ',"error");
      EnviarEmail('ChatBot Api error ',mensaje);
    }else{
      var mensaje =  l_json[i].Respuesta;
    }
      
      if (mensaje == '' || mensaje == null || mensaje == undefined ){
      }
      else{
    
        mensaje = mensaje.replaceAll("|","\n");
    
        console.log("mensaje "+message.from+" - "+mensaje);
        
        if(i<= cant_lim + jsonGlobal[indice][1] -1){
        
         await client.sendMessage(message.from,mensaje );
         await sleep(segundos);
         await io.emit('message', 'Respuesta: '+message.from+': '+ mensaje );
         if(tam_json-1==i){
            bandera_msg=1;
            jsonGlobal[indice][1] = 0;
            jsonGlobal[indice][2] = '';
            jsonGlobal[indice][3] = '';
         }
      }else{
       // for (var j = 0; j < 20; j++){
          msg_lim = msg_lim.replaceAll("|","\n");
        //}
        var msg_loc = msg_lim;

       
        if(tam_json  <= i + cant_lim  ){
          msg_loc = msg_loc.replace('<recuento>', tam_json  - i );
       }else{
        msg_loc = msg_loc.replace('<recuento>', cant_lim+1);
       }
      
        msg_loc = msg_loc.replace('<recuento_lote>', tam_json - 2);
        msg_loc = msg_loc.replace('<recuento_pendiente>', tam_json  - i);
              
        if (msg_loc == '' || msg_loc == null || msg_loc == undefined ){
        }
        else{
          client.sendMessage(message.from,msg_loc);
        }
       bandera_msg=0;
       jsonGlobal[indice][1]  = i;
       jsonGlobal[indice][3] = now;
       return;
      }
    }
  }


};

///////////////////////////////////////////////////////////////////////
// CONTROLA CADUCIDAD DE LOS MESNAJES
///////////////////////////////////////////////////////////////////////

async function controlar_hora_msg(){

  while(a < 1){
     for(var i in jsonGlobal){
     
      if( jsonGlobal[i][3] !== ''){
        var fecha = new Date();
        var fecha_msg = jsonGlobal[i][3].getTime();
        var fecha_msg2 = fecha.getTime();
        var diferencia = fecha_msg2-fecha_msg;
        if(diferencia > time_cad ){
          if(msg_cad == '' || msg_cad  == undefined || msg_cad == 0 ){
            
          } else {
            client.sendMessage(jsonGlobal[i][0],msg_cad );

          }
          console.log("timepo expirado "+ jsonGlobal[i][0]+' '+diferencia+' '+time_cad );
          // delete(jsonGlobal[i]);
          let now = new Date();
          jsonGlobal[i][3] = '';
          jsonGlobal[i][2] = '';
          jsonGlobal[i][1] = 0;
          }
        }

        
        
    }
   
    await sleep(5000);
  }   
}

 
function RecuperarJsonConfMensajes(){

  const jsonConf =  JSON.parse(fs.readFileSync(CONFIG_MAIN));
  const jsonError = JSON.parse(fs.readFileSync(CONFIG_ERRS));
 // console.log("configuracion.json "+jsonConf);

   
   seg_desde = jsonConf.configuracion.seg_desde;
   seg_hasta = jsonConf.configuracion.seg_hasta;
   dsn = jsonConf.configuracion.dsn;
   seg_msg = jsonConf.configuracion.seg_msg;
   api = jsonConf.configuracion.api;
   msg_inicio = jsonConf.configuracion.msg_inicio;
   msg_fin = jsonConf.configuracion.msg_fin;
   cant_lim = jsonConf.configuracion.cant_lim;
   time_cad = jsonConf.configuracion.time_cad;
   email_err = jsonError.configuracion.email_err;
   msg_lim = jsonConf.configuracion.msg_lim;
   msg_cad = jsonConf.configuracion.msg_cad;
   msg_can = jsonConf.configuracion.msg_can;

   smtp = jsonError.configuracion.smtp;
   email_usuario = jsonError.configuracion.user;
   email_pas = jsonError.configuracion.pass;
   email_puerto = jsonError.configuracion.puerto;
   email_saliente = jsonError.configuracion.email_sal;
   msg_errores = jsonError.configuracion.msg_error;
   nom_chatbot= jsonConf.configuracion.nom_emp;


}


async function EnviarEmail(subjet,texto){
/*
  texto = JSON.stringify(texto);
  console.log("email "+email_err);
  console.log("email2 "+subjet);
  console.log("email3 "+texto);

  subjet= nom_chatbot +" - "+subjet;

  let testAccount = await nodemailer.createTestAccount();

  let transporter = nodemailer.createTransport({
    host: smtp,
    port: email_puerto,
    secure: false, // true for 465, false for other ports
    auth: {
      user: email_usuario, // generated ethereal user
      pass: email_pas, // generated ethereal password
    },
  });
  
  let info = await transporter.sendMail({
    from: email_saliente, // sender address
    to: email_err, // list of receivers
    subject: subjet, // Subject line
    text: texto, // plain text body
    html: texto, // html body
  });

  console.log("Message sent: %s", info.messageId);
  
  console.log("Preview URL: %s", nodemailer.getTestMessageUrl(info));
 */
}

////////////////////////////////////////////////////////////////////////////////////////////
//  FUNCION PARA MANTENER EL JSON GLOBAL CON LOS TELEFONOS Y MENSAJES QUE VAN INGRESANDO - FUNCION
//   NECESARIA PARA PODER LIMITAR LA CANTIDAD DE MENSAJES CONTINUOS A ENVIAR
////////////////////////////////////////////////////////////////////////////////////////////

function recuperar_json(a_telefono, json){

  var indice =indexOf2d(a_telefono);


  let now = new Date();
 
  if(indice !== -1){
   // console.log("ESTA "+a_telefono);
   
    jsonGlobal[indice][0] = a_telefono;
   // jsonGlobal[a_telefono,2] = 0;
    jsonGlobal[indice][2] = json;
    jsonGlobal[indice][3] = now;
    //console.table(jsonGlobal);
 }else{

    //console.log("NO ESTA "  +a_telefono);
     
  jsonGlobal.push([a_telefono,0,json,now])
    
      
 }


}
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function detectMimeType(b64) {
  for (var s in signatures) {
    if (b64.indexOf(s) === 0) {
    return signatures[s];
  }
}}

function devolver_puerto(){

return port;
}


function devolver_seg_tele(){

return seg_tele;
}

function devolver_seg_desde(){

return seg_desde;
}

function devolver_seg_hasta(){

return seg_hasta;
}

function devolver_headless(){

return headless;
}

function RecuperarJsonConf(){
  
  const jsonConf =  JSON.parse(fs.readFileSync(CONFIG_MAIN));
  console.log("configuracion.json "+jsonConf.configuracion);

   port = jsonConf.configuracion.puerto;
   console.log("puerto: "+port);
 
   headless = jsonConf.configuracion.headless;
   console.log("headless: "+headless);
   seg_desde = jsonConf.configuracion.seg_desde;
   console.log("seg_desde: "+seg_desde);
   seg_hasta = jsonConf.configuracion.seg_hasta;
   console.log("seg_hasta: "+seg_hasta);
   dsn = jsonConf.configuracion.dsn;
   console.log("dsn: "+dsn);
   seg_msg = jsonConf.configuracion.seg_msg;
   console.log("seg_msg: "+seg_msg);
   seg_tele = jsonConf.configuracion.seg_tele;
   console.log("seg_msg: "+seg_tele);
   api = jsonConf.configuracion.api;
   console.log("api: "+api);
   msg_inicio = jsonConf.configuracion.msg_inicio
   console.log("msg_inicio: "+msg_inicio);
   msg_fin = jsonConf.configuracion.msg_fin
   console.log("msg_fin: "+msg_fin);
   if (headless == 'true'){
    headless = true
  }else
  {
    headless = false
  }

   // server.listen(process.env.PORT || port, function() {
  //console.log('App running on *: ' + port);
//});

}

function EscribirLog(mensaje,tipo){



// Función para escribir en el archivo de log

    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${mensaje}\n`;

    // Escribir el mensaje en el archivo (modo append)
    if(tipo == 'event'){
      fs.appendFile(logFilePath_event, logMessage, (err) => {
        if (err) {
            console.error('Error al escribir en el archivo de log:', err);
        } else {
            
        }
      });
    }else{
      fs.appendFile(logFilePath_error, logMessage, (err) => {
        if (err) {
            console.error('Error al escribir en el archivo de log:', err);
        } else {
            
        }
      });


    }



}

// ✅ Envío robusto de mensajes (texto/media). Resuelve LID y calienta el chat.
async function safeSendMessage(client, rawJid, content, options = {}, altJid = null) {
  // Intento primario
  await client.getChatById(rawJid).catch(() => {});
  try {
    return await client.sendMessage(rawJid, content, options);
  } catch (err) {
    // Fallback alternativo (por ejemplo, cambiar de @lid a @c.us)
    if (altJid && altJid !== rawJid) {
      await client.getChatById(altJid).catch(() => {});
      return await client.sendMessage(altJid, content, options);
    }
    throw err;
  }
}
// ✅ Convierte un JID cualquiera a uno "seguro" (usa @lid si existe)
async function resolveChatId(client, rawJid) {
  // Simplificado: evitamos usar LidUtils para no chocar con WaWebLidPnCache.
  // Usamos directamente el JID provisto y calentamos el chat en el envío.
  return rawJid;
}
