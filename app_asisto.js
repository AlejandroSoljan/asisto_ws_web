/*script:app_asisto_extendido*/
/*version:1.06.04   05/11/2025  (GPT + producto para precio)*/

// ──────────────────────────────────────────────────────────────────────────────
// MULTI-SESIÓN + PÁGINAS QR POR CUENTA
// ──────────────────────────────────────────────────────────────────────────────
const { fork } = require('child_process');
const IS_WORKER = process.env.CHILD_WORKER === '1';
function spawnAdditionalClientsIfNeeded() {
  if (IS_WORKER) return;
  const list = (process.env.WA_CLIENT_IDS || process.env.WA_CLIENT_ID || 'default')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  if (list.length <= 1) return;
  const MAX = Math.max(1, parseInt(process.env.MAX_WA_CLIENTS || '1', 10));
  const DELAY = Math.max(0, parseInt(process.env.WORKER_SPAWN_DELAY_MS || '20000', 10));
  const idsToSpawn = list.slice(1, 1 + (MAX - 1));
  idsToSpawn.forEach((id, idx) => {
    setTimeout(() => {
      const child = fork(process.argv[1], [], {
        env: {
          ...process.env,
          WA_CLIENT_ID: id,
          CHILD_WORKER: '1',
          SKIP_HTTP_SERVER: '1',
          NODE_OPTIONS: process.env.NODE_OPTIONS || '--max-old-space-size=256'
        }
      });
      child.on('message', (msg) => {
        try {
          global.__io && global.__io.emit(msg.type, msg.payload);
        } catch (_) {}
      });
      console.log('[MASTER] Spawned worker for', id);
    }, DELAY * (idx + 1));
  });
}
spawnAdditionalClientsIfNeeded();

// ====== VARIABLES DE ENTORNO ======
process.env.PUPPETEER_PRODUCT = process.env.PUPPETEER_PRODUCT || 'chrome';
process.env.PUPPETEER_CACHE_DIR = process.env.PUPPETEER_CACHE_DIR || '/var/data/.cache/puppeteer';
process.env.PUPPETEER_DOWNLOAD_PATH = process.env.PUPPETEER_DOWNLOAD_PATH || '/var/data/.cache/puppeteer';

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const puppeteer = require('puppeteer');
const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');
const os = require('os');
const express = require('express');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
const fetch = require('node-fetch');
const fileUpload = require('express-fileupload');
const axios = require('axios');
const mime = require('mime-types');
const utf8 = require('utf8');
const nodemailer = require('nodemailer');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';

const WA_CLIENT_ID = process.env.WA_CLIENT_ID || 'default';
const SESSIONS_DIR = process.env.SESSIONS_DIR || '/var/data/wwebjs';

// ──────────────────────────────────────────────────────────────────────────────
// util dirs
// ──────────────────────────────────────────────────────────────────────────────
function ensureWritableDir(dir) {
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.accessSync(dir, fs.constants.W_OK);
    console.log('[BOOT] Usando dataPath para LocalAuth →', dir);
  } catch (err) {
    console.error('[BOOT] No puedo escribir en', dir, '→', err.message);
    process.exit(1);
  }
}
ensureWritableDir(SESSIONS_DIR);
ensureWritableDir(process.env.PUPPETEER_CACHE_DIR);

// ========= RESOLVER/INSTALAR CHROME EN RUNTIME =========
function fileExists(p) { try { return p && fs.existsSync(p); } catch { return false; } }
function tryPuppeteerExecutablePath() {
  try { const p = puppeteer.executablePath(); return fileExists(p) ? p : null; } catch { return null; }
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
  try {
    console.log('[CHROME] Instalando Chrome…');
    execSync('npx puppeteer browsers install chrome', {
      stdio: 'inherit',
      env: { ...process.env, PUPPETEER_CACHE_DIR: process.env.PUPPETEER_CACHE_DIR }
    });
  } catch (e) {
    console.error('[CHROME] Falló la instalación automática:', e?.message || e);
  }
}
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
  let found = tryCommonSystemPaths() || tryPuppeteerExecutablePath();
  if (found) return found;
  installChromeIfNeeded();
  found = resolveInstalledChromePath() || tryPuppeteerExecutablePath();
  if (found) {
    process.env.PUPPETEER_EXECUTABLE_PATH = found;
    console.log('[CHROME] Usando ejecutable instalado →', found);
    return found;
  }
  console.warn('[CHROME] No se encontró Chrome tras instalación.');
  return undefined;
}

// ──────────────────────────────────────────────────────────────────────────────
// variables de config
// ──────────────────────────────────────────────────────────────────────────────
var a = 0;
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
var api2 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta_no_enviados";
var api3 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Actualiza_mensaje";
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
var jsonGlobal = [];
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
var Id_msj_dest;
var Id_msj_renglon;

// ──────────────────────────────────────────────────────────────────────────────
// APP / SOCKET
// ──────────────────────────────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = socketIO(server, { cors: { origin: "*" } });
global.__io = io;
app.use(fileUpload({ debug: false }));

app.get('/qr/:id', (req, res) => {
  const id = req.params.id || 'default';
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(`<!doctype html>
<html><head><meta charset="utf-8"><title>QR ${id}</title></head>
<body>
  <h3>QR para: ${id}</h3>
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
    socket.on('qr', (data)=>{
      if(!data || data.id !== id) return;
      img.src = data.url; img.style.display='block'; log('QR actualizado');
    });
    socket.on('message', (m)=>{ if((m||'').includes('Código QR') && (m||'').includes(id)) log(m); });
    socket.on('authenticated', (m)=>{ if((m||'').includes(id)) log(m); });
  </script>
</body></html>`);
});
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ──────────────────────────────────────────────────────────────────────────────
// funciones de config
// ──────────────────────────────────────────────────────────────────────────────
function RecuperarJsonConf() {
  const jsonConf = JSON.parse(fs.readFileSync('configuracion.json'));
  console.log("configuracion.json " + jsonConf.configuracion);

  port = jsonConf.configuracion.puerto;
  headless = jsonConf.configuracion.headless;
  seg_desde = jsonConf.configuracion.seg_desde;
  seg_hasta = jsonConf.configuracion.seg_hasta;
  dsn = jsonConf.configuracion.dsn;
  seg_msg = jsonConf.configuracion.seg_msg;
  seg_tele = jsonConf.configuracion.seg_tele;
  api = jsonConf.configuracion.api;
  msg_inicio = jsonConf.configuracion.msg_inicio;
  msg_fin = jsonConf.configuracion.msg_fin;
  cant_lim = jsonConf.configuracion.cant_lim;
  time_cad = jsonConf.configuracion.time_cad;
  msg_lim = jsonConf.configuracion.msg_lim;
  msg_cad = jsonConf.configuracion.msg_cad;
  msg_can = jsonConf.configuracion.msg_can;
  if (headless == 'true') headless = true; else headless = false;

  const jsonError = JSON.parse(fs.readFileSync('configuracion_errores.json'));
  smtp = jsonError.configuracion.smtp;
  email_usuario = jsonError.configuracion.user;
  email_pas = jsonError.configuracion.pass;
  email_puerto = jsonError.configuracion.puerto;
  email_saliente = jsonError.configuracion.email_sal;
  msg_errores = jsonError.configuracion.msg_error;
  nom_chatbot = jsonConf.configuracion.nom_emp;
}
function RecuperarJsonConfMensajes() { RecuperarJsonConf(); }

function EscribirLog(mensaje, tipo) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${mensaje}\n`;
  try { fs.appendFileSync('log.txt', logMessage); } catch (_) {}
  console.log(mensaje);
}

async function EnviarEmail(subjet, texto) { /* sin cambios */ }

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function indexOf2d(itemtofind) {
  var valor = -1;
  for (var i = 0; i < jsonGlobal.length; i++) {
    if (jsonGlobal[i][0] == itemtofind) return i;
  }
  return valor;
}
function recuperar_json(a_telefono, json) {
  var indice = indexOf2d(a_telefono);
  let now = new Date();
  if (indice !== -1) {
    jsonGlobal[indice][0] = a_telefono;
    jsonGlobal[indice][2] = json;
    jsonGlobal[indice][3] = now;
  } else {
    jsonGlobal.push([a_telefono, 0, json, now]);
  }
}

// helper para sacar número sin @c.us / @lid
function extraerNumeroWhatsapp(id) {
  if (!id) return '';
  return id.split('@')[0];
}

// ──────────────────────────────────────────────────────────────────────────────
// FUNCIÓN GPT: clasificar consulta + producto
// ──────────────────────────────────────────────────────────────────────────────
async function detectarIntencionConChatGPT(textoUsuario) {
  if (!OPENAI_API_KEY) {
    return { intent: 'fallback_api' };
  }

  const sistema = `
Sos un asistente de WhatsApp de una empresa.
Tenés que clasificar el mensaje del cliente.

Debes devolver SIEMPRE JSON puro con este formato:
{
  "intent": "basica" | "precio" | "otro",
  "producto": "texto del producto o variante detectada o ''",
  "respuesta_sugerida": "solo si intent=basica"
}

Reglas:
- intent = "basica": todo lo relacionado a la empresa y productos, en base al rol y contexto detallado ,o cuando no sabés o es muy genérico  → completá respuesta_sugerida.
- intent = "precio": solo cuando pide precio de algún producto.
  En ese caso, en "producto" poné SOLO el nombre o descripción corta del producto que detectaste, sin palabras de cortesía.
  Ej: "bomba presurizadora 1 hp", "helado 1kg", "esterilla 2x2", "KM 312".




  Rol / Contexto

Actuá como asesor comercial de Revell Plast, una empresa argentina fundada en 1985 con sucursales en Venado Tuerto, Rufino y Rosario, dedicada a la venta y distribución de productos de climatización, agua, baño, cocina, piscinas y bombas. La empresa trabaja con marcas reconocidas y es distribuidora de varias de ellas.

Marcas y líneas con las que trabajás

Trabajás con (pero no limitado a): Peisa (calderas, climatizadores de piscina, radiadores, toalleros, termostatos), Vasser (cocina y baño, accesorios), FV (griferías), Ferrum (sanitarios), Johnson Acero (bacha/mesada acero), Rotoplas, Termosol, Ecotermo, Rheem (agua caliente, termotanques, tanques), Rowa, Grundfos, DAB, Czerweny, Sylwan (bombas y presurización), Vulcano (piscinas), e hidromasajes / spa inflables Intex.

Cómo responder sobre productos

Siempre explicá la función del producto y su uso típico (vivienda, comercio, piscina, calefacción).

Resaltá las variables importantes según el rubro:

calefacción: potencia, tipo de caldera (dual / solo calefacción), rendimiento;

agua caliente: capacidad en litros, fuente de energía;

bombas: caudal, altura manométrica, aplicación (presurizar, elevar, riego);

baño/cocina: material, terminación, compatibilidad.

Si el cliente pide una alternativa, ofrecé otra marca/modelo dentro de las marcas que comercializa Revell Plast.


🔎 Muy importante – consultas técnicas específicas

Si el cliente pregunta algo muy específico de un producto (por ejemplo: curva de bombeo de un modelo puntual, manual de instalación, tensión exacta, ficha técnica, repuestos recomendados, compatibilidad con otro equipo, medidas exactas, caudal a determinada altura, presión nominal, capacidad del termotanque, certificado o norma), indicá que esa información debe buscarse en la página web oficial del fabricante o en la ficha técnica del modelo.
Decí algo como:
“Ese dato es propio del fabricante. Consultá la ficha técnica o el catálogo oficial de [marca] para ese modelo en su web, porque ahí está la información actualizada.”
Siempre que se pueda, nombrá la marca correcta (Peisa, FV, Ferrum, Rowa, Grundfos, etc.).

Tono y atención

Usá un tono profesional, claro y amable, como vendedor técnico de una casa de sanitarios/climatización. Podés cerrar invitando a contactar por WhatsApp o pasar por la sucursal.

`.trim();

  const body = {
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: sistema },
      { role: "user", content: textoUsuario }
    ],
    temperature: 0.2
  };

  try {
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });

    const data = await resp.json();
    let contenido = data.choices?.[0]?.message?.content?.trim() || "{}";
    // por si devuelve ```json
    contenido = contenido.replace(/```json/gi, '').replace(/```/g, '').trim();
    const parsed = JSON.parse(contenido);
    return parsed;
  } catch (err) {
    console.error("Error llamando a ChatGPT:", err?.message || err);
    return { intent: 'fallback_api' };
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// WHATSAPP CLIENT
// ──────────────────────────────────────────────────────────────────────────────
if (process.env.SKIP_HTTP_SERVER !== '1') {
  RecuperarJsonConf();
  server.listen(process.env.PORT || port, function () {
    console.log('App running on *: ' + port, '| host:', os.hostname());
    EscribirLog('App running on *: ' + port, "event");
  });
} else {
  RecuperarJsonConf();
}

const client = new Client({
  restartOnAuthFail: true,
  webVersionCache: { type: 'none' },
  puppeteer: {
    headless: true,
    protocolTimeout: 0,
    waitForInitialPage: true,
    executablePath: (getChromePathOrInstall() || process.env.PUPPETEER_EXECUTABLE_PATH || puppeteer.executablePath()),
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--no-zygote',
      '--single-process',
      '--renderer-process-limit=1',
      '--disable-gpu'
    ]
  },
  authStrategy: new LocalAuth({
    clientId: WA_CLIENT_ID,
    dataPath: SESSIONS_DIR
  })
});

// ──────────────────────────────────────────────────────────────────────────────
// CONSULTA PERIODICA (queda igual) ...
// ──────────────────────────────────────────────────────────────────────────────
async function ConsultaApiMensajes() {
  // lo dejo igual que tu versión anterior...
}

// ──────────────────────────────────────────────────────────────────────────────
// procesar_mensaje (queda igual)
// ─────────────────────────────────────────────────────────────────────────────-
async function procesar_mensaje(json, message) {
  RecuperarJsonConfMensajes();
  var indice = indexOf2d(message.from);
  let now = new Date();
  var segundos = Math.random() * (seg_hasta - seg_desde) + seg_desde;
  jsonGlobal[indice][3] = now;

  let tam_json = 0;
  for (var j in jsonGlobal[indice][2]) tam_json = tam_json + 1;

  for (var i = jsonGlobal[indice][1]; i < tam_json; i++) {
    let l_json = jsonGlobal[indice][2];
    var mensaje;
    if (l_json[i].cod_error) {
      mensaje = l_json[i].msj_error;
      EscribirLog('Error 05 - procesar_mensaje() devuelve cod_error API ', "error");
      EnviarEmail('ChatBot Api error ', mensaje);
    } else {
      mensaje = l_json[i].Respuesta;
    }
    if (mensaje) {
      mensaje = mensaje.replaceAll("|", "\n");
      if (i <= cant_lim + jsonGlobal[indice][1] - 1) {
        await client.sendMessage(message.from, mensaje);
        await sleep(segundos);
        await io.emit('message', 'Respuesta: ' + message.from + ': ' + mensaje);
      } else {
        jsonGlobal[indice][1] = i;
        bandera_msg = 1;
        if (msg_lim && msg_lim != '') {
          await client.sendMessage(message.from, msg_lim);
        } else {
          await client.sendMessage(message.from, 'Continuar? S / N');
        }
        break;
      }
    }
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// ÚNICO client.on('message')
// ─────────────────────────────────────────────────────────────────────────────-
client.on('message', async (message) => {
  const from = message.from;
  const bodyOrig = (message.body || '').trim();
  console.log("mensaje " + from + ': ' + bodyOrig);
  await io.emit('message', 'Mensaje: ' + from + ': ' + bodyOrig);

  // filtros
  if (from === 'status@broadcast') return;
  if (message.type !== 'chat') return;
  if (!message.to || !message.from) return;

  // tu filtro actual
  if (message.from == '5493462514448@c.us') {

    // flujo S/N
    let indice_telefono = indexOf2d(from);
    let valor_i = (indice_telefono === -1) ? 0 : jsonGlobal[indice_telefono][1];
    let bodyMay = bodyOrig.toUpperCase();
    if (valor_i !== 0) {
      if (bodyMay === 'N') {
        if (msg_can && msg_can !== '') {
          await client.sendMessage(from, msg_can);
        }
        jsonGlobal[indice_telefono][2] = '';
        jsonGlobal[indice_telefono][1] = 0;
        jsonGlobal[indice_telefono][3] = '';
        return;
      } else if (bodyMay === 'S') {
        await procesar_mensaje(jsonGlobal[indice_telefono][2], message);
        return;
      } else {
        await client.sendMessage(from, '🤔 *No entiendo*, \nPor favor ingrese *S* o *N* para mostrar los siguientes resultados');
        return;
      }
    }

    // 1) pasamos por GPT
    let analisis = await detectarIntencionConChatGPT(bodyOrig);
    console.log("Intención detectada:", analisis);

    if (analisis.intent === 'basica' && analisis.respuesta_sugerida) {
      await client.sendMessage(from, analisis.respuesta_sugerida);
      return;
    }

    // 2) vamos al API, pero si es precio mandamos solo el producto detectado
    RecuperarJsonConfMensajes();

    // Tel_Origen lo sacamos del contacto, si no, del id
    let telefonoFrom = '';
    try {
      const contact = await client.getContactById(message.from);
      telefonoFrom = contact?.number || '';
    } catch (e) {
      console.log('No pude obtener contact.number, uso from:', e?.message || e);
    }
    if (!telefonoFrom) {
      telefonoFrom = extraerNumeroWhatsapp(message.from);
    }

    // Tel_Destino del propio mensaje
    //const telefonoTo = extraerNumeroWhatsapp(message.to);

    const telefonoTo = '5493462616000'

    // mensaje que va al API
    let mensajeParaApi = bodyOrig;
    if (analisis.intent === 'precio' && analisis.producto) {
      // 👉 acá está el cambio que pediste
      mensajeParaApi = analisis.producto;
    }

    const jsonTexto = {
      Tel_Origen: telefonoFrom ?? "",
      Tel_Destino: telefonoTo ?? "",
      Mensaje: mensajeParaApi ?? "",
      Fecha: new Date().toISOString().slice(0, 19).replace('T', ' ')
    };

    const url_api = api;
    console.log("Conectando a API -> " + url_api, "con payload:", jsonTexto);
    EscribirLog("Conectando a API -> " + url_api, "event");

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 55000);

    try {
      const resp = await fetch(url_api, {
        method: "POST",
        body: JSON.stringify(jsonTexto),
        headers: { "Content-Type": "application/json" },
        signal: controller.signal
      });
      clearTimeout(timeoutId);

      const jsonResp = await resp.json();
      console.log("API devolvió:", JSON.stringify(jsonResp));
      recuperar_json(from, jsonResp);
      await procesar_mensaje(jsonResp, message);
    } catch (err) {
      clearTimeout(timeoutId);
      const detalle = "Error 03 Chatbot Error " + (err?.message || err) + " " + JSON.stringify(jsonTexto);
      console.log(detalle);
      EscribirLog(detalle, "error");
    }
  }
});

// ──────────────────────────────────────────────────────────────────────────────
// eventos de estado
// ─────────────────────────────────────────────────────────────────────────────-
client.initialize();

client.on('ready', async () => {
  console.log("listo ready.");
  telefono_qr = client.info.me.user;
  console.log("TEL QR: " + client.info.me.user);
  await io.emit('message', 'Whatsapp Listo!');
  EscribirLog('Whatsapp Listo!', "event");
  // ConsultaApiMensajes();
});

client.on('qr', (qr) => {
  console.log('QR RECEIVED', qr);
  qrcode.toDataURL(qr, (err, url) => {
    const payload = { id: (process.env.WA_CLIENT_ID || 'default'), url };
    io.emit('qr', payload);
    io.emit('message', `[${process.env.WA_CLIENT_ID || 'default'}] Código QR Recibido, por favor escanea`);
  });
});

client.on('authenticated', async () => {
  io.emit('authenticated', `[${process.env.WA_CLIENT_ID || 'default'}] Whatsapp Autenticado!.`);
  io.emit('message', `[${process.env.WA_CLIENT_ID || 'default'}] Whatsapp Autenticado!`);
  console.log('Autenticado');
  EscribirLog('Autenticado', "event");
});

client.on('auth_failure', function (session) {
  io.emit('message', 'Auth failure, restarting.');
  EscribirLog('Error 04 - Chatbot error Auth failure', "error");
});

client.on('disconnected', (reason) => {
  io.emit('message', 'Whatsapp Desconectado!');
  EscribirLog('Chatbot Desconectado ', "event");
  client.initialize();
});

// helper dummy
function detectMimeType(b64) { return 'application/octet-stream'; }
function devolver_seg_tele() { return seg_tele; }
function devolver_seg_desde() { return seg_desde; }
function devolver_seg_hasta() { return seg_hasta; }
function devolver_headless() { return headless; }
function devolver_puerto() { return port; }
