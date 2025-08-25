/*script:app_asisto - Render Free (Node nativo)
  Basado en tu lógica original; ajustado para puppeteer (descarga Chromium) y sesión local.
*/
const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');
const express = require('express');
const { body, validationResult } = require('express-validator');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
const fetch = require('node-fetch');
const fileUpload = require('express-fileupload');
const axios = require('axios');
const mime = require('mime-types');
const utf8 = require('utf8');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');

// -------------------- Variables y config --------------------
let a = 0;
let headless = true;
let seg_desde = 80000;
let seg_hasta = 10000;
let seg_msg = 5000;
let seg_tele = 3000;
let version = "1.0";
let script = "__";
let telefono_qr = "0";
let telefono_local = "0";
let tel_array = [];
let ver_whatsapp = "0";
let dsn = "msm_manager";

let api  = "http://managermsm.ddns.net:2002/v200/api/Api_Chat_Cab/ProcesarMensajePost";
let api2 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta_no_enviados";
let api3 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Actualiza_mensaje";

let key = 'FMM0325*';
let msg_inicio = "";
let msg_fin = "";
let cant_lim = 0;
let msg_lim = 'Continuar? S / N';
let time_cad = 0;
let email_err = "";
let msg_cad = "";
let msg_can = "";
let bandera_msg = 1;

let jsonGlobal = [];   // [ 0:tel, 1:i, 2:json, 3:hora ]
let msg_errores, nom_chatbot;

let Id_msj_dest;
let Id_msj_renglon;

const logFilePath_event = path.join(__dirname, 'app_asisto_event.log');
const logFilePath_error = path.join(__dirname, 'app_asisto_error.log');

EscribirLog("inicio Script","event");

// ---------- Express + Socket ----------
const app = express();
const server = http.createServer(app);
const io = socketIO(server, { cors: { origin: "*" } });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(fileUpload({ debug: false }));

// Sirve index.html (poné tu HTML en el root o crea /public y cambialo)
app.get('/', (req, res) => {
  res.sendFile('index.html', { root: __dirname });
});

// Lee configuración inicial desde archivos
RecuperarJsonConf();         // setea headless, seg_desde, etc.
RecuperarJsonConfMensajes(); // textos y errores

// Render asigna PORT; fallback 3000 en local
const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log('App running on *: ' + PORT);
  EscribirLog('App running on *: ' + PORT, "event");
});

// -------------------- WhatsApp Client --------------------
// Sesión local en ./sessions (no /data). NO usar env SESSION_PATH en Render.
const SESSION_PATH = path.join(__dirname, 'sessions');
try { fs.mkdirSync(SESSION_PATH, { recursive: true }); } catch {}

const client = new Client({
  restartOnAuthFail: true,
  puppeteer: {
    headless: true,                // en Render Free conviene headless
    // Importante: NO poner executablePath -> puppeteer usa su Chromium descargado
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-gpu'
    ],
  },
  authStrategy: new LocalAuth({ dataPath: SESSION_PATH })
});

// -------------------- Aux: firmas mime b64 --------------------
const signatures = {
  JVBERi0: "application/pdf",
  R0lGODdh: "image/gif",
  R0lGODlh: "image/gif",
  iVBORw0KGgo: "image/png",
  "/9j/": "image/jpg"
};

// -------------------- API: consulta y envío --------------------
async function ConsultaApiMensajes(){
  console.log("Consultando a API ");
  const url =  `${api2}?key=${key}&nro_tel_from=${telefono_qr}`;
  const url_confirma_msg = `${api3}?key=${key}&nro_tel_from=${telefono_qr}`;
  console.log("Conectando a API "+url);
  EscribirLog("Conectando a API "+url,"event");

  await sleep(1000);

  let intentos = 0;
  while (intentos < 10) {
    RecuperarJsonConfMensajes();
    seg_msg = Math.random() * (devolver_seg_hasta() - devolver_seg_desde()) + devolver_seg_desde();

    try {
      // AbortController por request
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 55000);

      const resp = await fetch(url, { method: "GET", signal: controller.signal });
      clearTimeout(timeoutId);

      if (resp.ok) {
        const data = await resp.json();

        const tam_destinatarios = Object.keys(data[0]?.destinatarios || {}).length;
        for (let i = 0; i < tam_destinatarios; i++) {
          const idRenglon = data[0].destinatarios[i].Id_msj_renglon;

          // Mensajes de ese renglón
          const respuesta = (data[0].mensajes || []).filter(m => m.Id_msj_renglon == idRenglon);
          for (let j = 0; j < respuesta.length; j++) {
            Id_msj_dest    = data[0].destinatarios[i].Id_msj_dest;
            Id_msj_renglon = data[0].destinatarios[i].Id_msj_renglon;

            const Nro_tel         = data[0].destinatarios[i].Nro_tel;
            const Nro_tel_format  = `${Nro_tel}@c.us`;
            const Msj             = respuesta[j].Msj || '';
            const contenido       = respuesta[j].Content;
            let   Content_nombre  = respuesta[j].Content_nombre || 'archivo';

            console.log('--------------------------------------------------');
            console.log("Id_msj_dest "+Id_msj_dest);
            console.log("Id_msj_renglon "+Id_msj_renglon);
            console.log("Nro_tel "+Nro_tel);
            console.log("Msj "+Msj);
            console.log("Content_nombre "+Content_nombre);
            console.log('--------------------------------------------------');

            if (isNaN(Number(Nro_tel))) {
              console.log("numero invalido");
              io.emit('message', `Mensaje: ${Nro_tel_format}: Número Inválido`);
              await actualizar_estado_mensaje(url_confirma_msg, 'I', null, null, null, null, null, Id_msj_renglon, Id_msj_dest);
              continue;
            }

            const isRegistered = await client.isRegisteredUser(Nro_tel_format).catch(() => false);
            if (!isRegistered) {
              EscribirLog(`Mensaje: ${Nro_tel_format}: Número no Registrado`, "event");
              console.log("numero no registrado");
              io.emit('message', `Mensaje: ${Nro_tel_format}: Número no Registrado`);
              await actualizar_estado_mensaje(url_confirma_msg, 'I', null, null, null, null, null, Id_msj_renglon, Id_msj_dest);
              continue;
            }

            // Envío
            if (contenido) {
              const media = new MessageMedia(detectMimeType(contenido), contenido, Content_nombre);
              io.emit('message', `Mensaje: ${Nro_tel_format}: ${Msj}`);
              await client.sendMessage(Nro_tel_format, media, { caption: Msj });
            } else {
              io.emit('message', `Mensaje: ${Nro_tel_format}: ${Msj}`);
              await client.sendMessage(Nro_tel_format, Msj);
            }

            // Datos de contacto
            try {
              const contact = await client.getContactById(Nro_tel_format);
              let tipo, contacto, email, direccion, nombre;
              if (contact.isBusiness === true) {
                tipo = 'B';
                contacto = contact.pushname;
                email = contact.businessProfile?.email || null;
                direccion = contact.businessProfile?.address || null;
                nombre = contact.name;
              } else {
                tipo = 'C';
                nombre = contact.name;
                contacto = contact.shortName;
                direccion = null;
                email = null;
              }
              await actualizar_estado_mensaje(url_confirma_msg, 'E', tipo, nombre, contacto, direccion, email, Id_msj_renglon, Id_msj_dest);
            } catch (e) {
              console.log('No se pudo obtener contacto: ' + e.message);
            }

            await sleep(seg_msg);
          }
        }

      } else {
        const raw = await resp.text().catch(() => '');
        if (msg_errores) {
          console.log("ApiWhatsapp - Response ERROR " + raw);
          EscribirLog("ApiWhatsapp - Response ERROR " + raw, "error");
        }
      }

    } catch (err) {
      console.log(err);
      EscribirLog(err, "error");
    }

    RecuperarJsonConfMensajes();
    seg_tele = devolver_seg_tele();
    await sleep(seg_tele);
    intentos++;
  }
}

// Marca estado en API (stub simple; mantené tu implementación real si la tenías)
async function actualizar_estado_mensaje(url, estado, tipo, nombre, contacto, direccion, email, idRenglon, idDest) {
  try {
    const payload = {
      estado, tipo, nombre, contacto, direccion, email,
      Id_msj_renglon: idRenglon, Id_msj_dest: idDest
    };
    await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
  } catch (e) {
    console.log('Error actualizando estado: ' + e.message);
  }
}

// -------------------- Handlers de mensajes entrantes --------------------
client.on('message', async (message) => {
  const indice_telefono = indexOf2d(message.from);
  const valor_i = (indice_telefono === -1) ? 0 : jsonGlobal[indice_telefono][1];

  EscribirLog(`${message.from} ${message.to} ${message.type} ${message.body}`, "event");
  console.log("mensaje " + message.from);

  if (valor_i === 0) {
    RecuperarJsonConfMensajes();

    let telefonoTo = (message.to || '').replace('@c.us','');
    let telefonoFrom = (message.from || '').replace('@c.us','');

    if (telefonoFrom === 'status@broadcast') return;
    if (message.type !== 'chat') return;
    if (!message.to || !message.from) return;

    if (msg_inicio) client.sendMessage(message.from, msg_inicio);
    io.emit('message', `Mensaje: ${message.from}: ${message.body}`);

    let jsonTexto = {
      Tel_Origen: telefonoFrom ?? "",
      Tel_Destino: telefonoTo ?? "",
      Mensaje: message?.body ?? "",
      Respuesta: ""
    };

    const url = api;
    EscribirLog("Mensaje " + JSON.stringify(jsonTexto), 'event');

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 55000);

      const resp = await fetch(url, {
        method: "POST",
        body: JSON.stringify(jsonTexto),
        headers: { "Content-type": "application/json; charset=UTF-8" },
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      const raw = await resp.text();
      let data = null;
      try { data = raw ? JSON.parse(raw) : null; } catch {}

      if (!resp.ok) {
        const detalle = data ? JSON.stringify(data) : raw;
        EscribirLog("Error 02 ApiWhatsapp - Response ERROR " + detalle, "error");
        EnviarEmail("ApiWhatsapp - Response ERROR ", detalle);
        if (msg_errores) await client.sendMessage(message.from, msg_errores);
        return;
      }

      recuperar_json(message.from, data);
      await procesar_mensaje(data, message);

      if (msg_fin) await client.sendMessage(message.from, msg_fin);
      return;

    } catch (err) {
      const detalle = "Error 03 Chatbot Error " + (err?.message || err) + " " + JSON.stringify(jsonTexto);
      console.log(detalle);
      // Podés notificar si querés:
      // EscribirLog(detalle, "error"); EnviarEmail("Chatbot Error ", detalle);
    }
  }

  // Manejo de lote S/N
  let body = (message.body || '').trim().toUpperCase();
  if (valor_i !== 0 && body === 'N') {
    if (msg_can) await client.sendMessage(message.from, msg_can);
    bandera_msg = 1;
    jsonGlobal[indice_telefono][2] = '';
    jsonGlobal[indice_telefono][1] = 0;
    jsonGlobal[indice_telefono][3] = '';
  }
  if (valor_i !== 0 && (body !== 'N' && body !== 'S')) {
    await client.sendMessage(message.from, '🤔 *No entiendo*, \nPor favor ingrese *S* o *N* para mostrar los siguientes resultados\n ');
  }
  if (valor_i !== 0 && body === 'S') {
    await procesar_mensaje(jsonGlobal[indice_telefono][2], message);
  }
});

// -------------------- Eventos de cliente --------------------
client.on('ready', async () => {
  console.log("listo ready....");
  try { telefono_qr = client.info?.me?.user || telefono_qr; } catch {}
  console.log("TEL QR: " + telefono_qr);
  io.emit('message', 'Whatsapp Listo!');
  EscribirLog('Whatsapp Listo!',"event");

  // Si querés arrancar el polling:
  // ConsultaApiMensajes();
});

client.on('qr', (qr) => {
  console.log('QR RECEIVED');
  qrcode.toDataURL(qr, (err, url) => {
    io.emit('qr', url);
    io.emit('message', 'Código QR Recibido...');
  });
});

client.on('authenticated', async () => {
  io.emit('authenticated', 'Whatsapp Autenticado!.');
  io.emit('message', 'Whatsapp Autenticado!');
  console.log('Autenticado');
  EscribirLog('Autenticado',"event");
});

client.on('auth_failure', function() {
  io.emit('message', 'Auth failure, restarting...');
  EnviarEmail('Chatbot error Auth failure','Auth failure, restarting...');
  EscribirLog('Error 04 - Chatbot error Auth failure','Auth failure, restarting...',"error");
});

client.on('disconnected', (reason) => {
  io.emit('message', 'Whatsapp Desconectado!');
  EnviarEmail('Chatbot Desconectado ','Desconectando...');
  EscribirLog('Chatbot Desconectado ','Desconectando...',"event");
  client.destroy();
  client.initialize();
});

// -------------------- Procesamiento y utilidades --------------------
function indexOf2d(itemtofind) {
  for (let i = 0; i < jsonGlobal.length; i++) {
    if (jsonGlobal[i][0] === itemtofind) return i;
  }
  return -1;
}

function recuperar_json(a_telefono, data){
  const indice = indexOf2d(a_telefono);
  const now = new Date();
  if (indice !== -1) {
    jsonGlobal[indice][0] = a_telefono;
    jsonGlobal[indice][2] = data;
    jsonGlobal[indice][3] = now;
  } else {
    jsonGlobal.push([a_telefono, 0, data, now]);
  }
}

async function procesar_mensaje(data, message){
  RecuperarJsonConfMensajes();

  const indice = indexOf2d(message.from);
  const now = new Date();

  const segundos = Math.random() * (seg_hasta - seg_desde) + seg_desde;
  const l_json = jsonGlobal[indice][2];

  jsonGlobal[indice][3] = now;

  // tamaño del arreglo de respuestas
  let tam_json = 0;
  for (let _ in l_json) tam_json++;

  for (let i = jsonGlobal[indice][1]; i < tam_json; i++) {
    let mensaje;
    if (l_json[i].cod_error) {
      mensaje = l_json[i].msj_error;
      EscribirLog('Error 05 - procesar_mensaje() devuelve cod_error API ', "error");
      EnviarEmail('ChatBot Api error ', mensaje);
    } else {
      mensaje = l_json[i].Respuesta;
    }

    if (!mensaje) continue;
    mensaje = String(mensaje).replaceAll("|","\n");

    if (i <= cant_lim + jsonGlobal[indice][1] - 1) {
      await client.sendMessage(message.from, mensaje);
      await sleep(segundos);
      io.emit('message', `Respuesta: ${message.from}: ${mensaje}`);
      if (tam_json - 1 === i) {
        bandera_msg = 1;
        jsonGlobal[indice][1] = 0;
        jsonGlobal[indice][2] = '';
        jsonGlobal[indice][3] = '';
      }
    } else {
      let msg_loc = String(msg_lim || '').replaceAll("|","\n");
      const pendiente = tam_json - i;
      msg_loc = msg_loc.replace('<recuento>', Math.min(pendiente, cant_lim+1));
      msg_loc = msg_loc.replace('<recuento_lote>', tam_json - 2);
      msg_loc = msg_loc.replace('<recuento_pendiente>', pendiente);
      if (msg_loc) await client.sendMessage(message.from, msg_loc);
      bandera_msg = 0;
      jsonGlobal[indice][1] = i;
      jsonGlobal[indice][3] = now;
      return;
    }
  }
}

async function controlar_hora_msg(){
  while (a < 1) {
    for (let i in jsonGlobal) {
      if (jsonGlobal[i][3] !== '') {
        const fecha_msg = jsonGlobal[i][3].getTime();
        const diferencia = Date.now() - fecha_msg;
        if (diferencia > time_cad) {
          if (msg_cad) await client.sendMessage(jsonGlobal[i][0], msg_cad);
          jsonGlobal[i][3] = '';
          jsonGlobal[i][2] = '';
          jsonGlobal[i][1] = 0;
        }
      }
    }
    await sleep(5000);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function detectMimeType(b64) {
  for (let s in signatures) {
    if (b64.indexOf(s) === 0) return signatures[s];
  }
  // fallback
  return 'application/octet-stream';
}

// -------------------- Config desde archivos --------------------
function RecuperarJsonConfMensajes(){
  const jsonConf  = JSON.parse(fs.readFileSync('configuracion.json'));
  const jsonError = JSON.parse(fs.readFileSync('configuracion_errores.json'));

  seg_desde   = jsonConf.configuracion.seg_desde;
  seg_hasta   = jsonConf.configuracion.seg_hasta;
  dsn         = jsonConf.configuracion.dsn;
  seg_msg     = jsonConf.configuracion.seg_msg;
  api         = jsonConf.configuracion.api;
  msg_inicio  = jsonConf.configuracion.msg_inicio;
  msg_fin     = jsonConf.configuracion.msg_fin;
  cant_lim    = jsonConf.configuracion.cant_lim;
  time_cad    = jsonConf.configuracion.time_cad;
  msg_lim     = jsonConf.configuracion.msg_lim;
  msg_cad     = jsonConf.configuracion.msg_cad;
  msg_can     = jsonConf.configuracion.msg_can;
  seg_tele    = jsonConf.configuracion.seg_tele;
  nom_chatbot = jsonConf.configuracion.nom_emp;

  email_err     = jsonError.configuracion.email_err;
  smtp          = jsonError.configuracion.smtp;
  email_usuario = jsonError.configuracion.user;
  email_pas     = jsonError.configuracion.pass;
  email_puerto  = jsonError.configuracion.puerto;
  email_saliente= jsonError.configuracion.email_sal;
  msg_errores   = jsonError.configuracion.msg_error;
}

function RecuperarJsonConf(){
  const jsonConf = JSON.parse(fs.readFileSync('configuracion.json'));
  headless   = (String(jsonConf.configuracion.headless).toLowerCase() === 'true');
  seg_desde  = jsonConf.configuracion.seg_desde;
  seg_hasta  = jsonConf.configuracion.seg_hasta;
  dsn        = jsonConf.configuracion.dsn;
  seg_msg    = jsonConf.configuracion.seg_msg;
  seg_tele   = jsonConf.configuracion.seg_tele;
  api        = jsonConf.configuracion.api;
  msg_inicio = jsonConf.configuracion.msg_inicio;
  msg_fin    = jsonConf.configuracion.msg_fin;
}

async function EnviarEmail(subjet,texto){
  // Si querés habilitar envío real, completá SMTP en configuracion_errores.json y descomentá:
  /*
  subjet = `${nom_chatbot} - ${subjet}`;
  let transporter = nodemailer.createTransport({
    host: smtp, port: email_puerto, secure: false,
    auth: { user: email_usuario, pass: email_pas },
  });
  await transporter.sendMail({ from: email_saliente, to: email_err, subject: subjet, text: texto, html: texto });
  */
}

// -------------------- Logs --------------------
function EscribirLog(mensaje,tipo){
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${mensaje}\n`;
  const file = (tipo === 'event') ? logFilePath_event : logFilePath_error;
  fs.appendFile(file, logMessage, (err) => { if (err) console.error('Error al escribir log:', err); });
}

// -------------------- Inicializa --------------------
client.initialize();
