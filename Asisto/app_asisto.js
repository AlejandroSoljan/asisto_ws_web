/*script:app_asisto*/
/*version:3.00.01   26/01/2026*/

//const chatbot = require("./funciones_asisto.js")
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const express = require('express');
const { body, validationResult } = require('express-validator');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
//var odbc = require("odbc");
const fetch = require('node-fetch');
const fileUpload = require('express-fileupload');
const axios = require('axios');
const mime = require('mime-types');
const { ClientInfo } = require('whatsapp-web.js/src/structures');
const utf8 = require('utf8');
//const { OdbcError } = require('odbc');
const nodemailer = require('nodemailer');
const { eventNames } = require('process');
const fs = require('fs');
const path = require('path');
const AR_TZ = 'America/Argentina/Cordoba';
// Overrides por cliente (desde PM2 / Sheet)

// -------------------------------
// Overrides por cliente (cliente.json)
// -------------------------------
// A partir de ahora, los overrides de "entorno necesarios" se leen desde cliente.json
// (si el archivo no existe o falta alguna clave, se usan defaults)
let CLIENTE_CONF = {};
try {
  const clientePath = path.join(__dirname, "cliente.json");
  if (fs.existsSync(clientePath)) {
    CLIENTE_CONF = JSON.parse(fs.readFileSync(clientePath, "utf8")) || {};
  }
} catch (e) {
  CLIENTE_CONF = {};
}

// cliente.json puede venir como:
//  A) { "configuracion": { ... } , "tenantId": "...", "clienteId":"...", "mongo_uri":"..." }
//  B) { ...todo plano... }
const CLIENTE_CFG = (CLIENTE_CONF && CLIENTE_CONF.configuracion && typeof CLIENTE_CONF.configuracion === "object")
  ? CLIENTE_CONF.configuracion
  : CLIENTE_CONF;


// Compat: aceptamos claves en español/inglés
const CLIENT_ID = String(CLIENTE_CONF.clienteId || CLIENTE_CONF.clientId || "default");
const SESSION_PATH = String(CLIENTE_CONF.sessionPath || CLIENTE_CONF.session_path || path.join(__dirname, ".wwebjs_auth"));
const CHROME_PATH = String(CLIENTE_CONF.chromePath || CLIENTE_CONF.chrome_path || ""); // opcional


// -------------------------------
// Multi-tenant (por cliente)
// - Se usa para particionar sesiones en Mongo (tenantId + clientId)
// - Se toma de cliente.json; si no está, fallback a configuracion.json si existe
// -------------------------------
let TENANT_ID = String(CLIENTE_CONF.tenantId || CLIENTE_CONF.tenant_id || "").trim();

if (!TENANT_ID) TENANT_ID = CLIENT_ID; // último fallback (no rompe compat)

// Overrides operativos salen de CLIENTE_CFG (configuracion)
const OVERRIDE_PORT = CLIENTE_CFG.port || CLIENTE_CFG.puerto || "";
const OVERRIDE_HEADLESS = String(CLIENTE_CFG.headless ?? "").toLowerCase();
const OVERRIDE_API_URL = CLIENTE_CFG.apiUrl || CLIENTE_CFG.api_url || CLIENTE_CFG.api || "";
 
 // -------------------------------
 // MongoDB (opcional) - sesiones + panel QR
 // -------------------------------
 const MONGO_URI = String(CLIENTE_CONF.mongoUri || CLIENTE_CONF.mongo_uri || "");
 const MONGO_DB  = String(CLIENTE_CONF.mongoDb || CLIENTE_CONF.mongo_db || ""); // opcional, si viene en URI se ignora
 const MONGO_CONNECT_TIMEOUT_MS = parseInt(String(CLIENTE_CONF.mongoConnectTimeoutMs || CLIENTE_CONF.mongo_connect_timeout_ms || "5000"), 10);
 const MONGO_BUCKET = String(CLIENTE_CONF.mongoBucket || CLIENTE_CONF.mongo_bucket || "wa_sessions_fs");
 const MONGO_SESSIONS_COLLECTION = String(CLIENTE_CONF.mongoSessionsCollection || CLIENTE_CONF.mongo_sessions_collection || "wa_sessions");
 
 // Intentamos dependencias sin romper si no existen
 let MongoClient, GridFSBucket, ObjectId;
 try {
   ({ MongoClient, GridFSBucket, ObjectId } = require("mongodb"));
 } catch (e) {
   // si no está mongodb, seguimos 100% local
 }
 
 let AdmZip = null;
 try {
   AdmZip = require("adm-zip");
 } catch (e) {
   // adm-zip no es obligatorio (solo para backup/restore)
 }
 
 let mongoReady = false;
 let mongoClient = null;
 let mongoDb = null;
 let gridfsBucket = null;
 
 // Estado para panel QR (socket + endpoints)
 let lastQrRaw = null;          // string QR
 let lastQrDataUrl = null;      // data:image/png;base64,...
 let lastQrAt = null;           // Date

var a = 0;
var port = 8002
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
console.log("VERSION APP: v2.00.01");

const app = express();
const server = http.createServer(app);
const io = socketIO(server);

app.use(express.json());
app.use(express.urlencoded({
  extended: true
}));

// Tenant actual (para debug/panel)
EscribirLog(`TENANT_ID runtime: ${TENANT_ID}`, "event");

app.use(fileUpload({
  debug: false
}));

app.get('/', (req, res) => {
  res.sendFile('index.html', {
    root: __dirname
  });
});

 
 // -------------------------------
 // Endpoints para "panel web de sesiones"
 // (sirven tanto con Mongo como sin Mongo)
// -------------------------------
 app.get('/api/sessions', async (req, res) => {
  try {
     // Permite override por querystring (por si querés un panel superadmin)
     const tenantReq = String(req.query.tenantId || req.query.tenant || TENANT_ID || "").trim();


    // Si hay Mongo, devolvemos la lista desde DB
     if (mongoReady && mongoDb) {
       const col = mongoDb.collection(MONGO_SESSIONS_COLLECTION);
       const q = tenantReq ? { tenantId: tenantReq } : {};
       const docs = await col.find(q, { projection: { _id: 0 } }).sort({ updatedAt: -1 }).toArray();
       return res.json({ ok: true, source: "mongo", sessions: docs });
     }
 
     // Fallback: inspecciona la carpeta LocalAuth
     // LocalAuth crea: <SESSION_PATH>/session-<CLIENT_ID>
     let sessions = [];
     try {
       if (fs.existsSync(SESSION_PATH)) {
         const items = fs.readdirSync(SESSION_PATH, { withFileTypes: true });
         sessions = items
           .filter(d => d.isDirectory() && d.name.startsWith("session-"))
           .map(d => ({
             tenantId: tenantReq || TENANT_ID,
             status: (d.name.replace(/^session-/, "") === CLIENT_ID) ? "local" : "local_other",
             telefono_qr: (d.name.replace(/^session-/, "") === CLIENT_ID) ? telefono_qr : undefined,
             updatedAt: null
           }));
       }
     } catch (e) { /* ignore */ }
 
     return res.json({ ok: true, source: "local", sessions });
   } catch (e) {
     return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
 });
 
 // QR del cliente actual (para panel)
 app.get('/api/sessions/me/qr', async (req, res) => {
   try {
     const payload = {
      tenantId: TENANT_ID,
       clientId: CLIENT_ID,
       telefono_qr,
       status: await safeGetClientState(),
       qrDataUrl: lastQrDataUrl,
       qrAt: lastQrAt ? lastQrAt.toISOString() : null,
       mongoReady
     };
 
     // Si hay Mongo, trae lo último persistido (si existe)
    if (mongoReady && mongoDb) {
       const col = mongoDb.collection(MONGO_SESSIONS_COLLECTION);
       const doc = await col.findOne({ tenantId: TENANT_ID, clientId: CLIENT_ID }, { projection: { _id: 0 } });
       if (doc) return res.json({ ok: true, ...doc, ...payload });
     }

     return res.json({ ok: true, ...payload });
   } catch (e) {
     return res.status(500).json({ ok: false, error: String(e?.message || e) });
   }
 });
 
// QR por clientId (panel multi-sesión)
 app.get('/api/sessions/:clientId/qr', async (req, res) => {
   try {
     const cid = String(req.params.clientId || "");
     if (!cid) return res.status(400).json({ ok: false, error: "clientId requerido" });
    const tenantReq = String(req.query.tenantId || req.query.tenant || TENANT_ID || "").trim();
     // Mongo primero
    if (mongoReady && mongoDb) {
       const col = mongoDb.collection(MONGO_SESSIONS_COLLECTION);
       const q = { clientId: cid };
       if (tenantReq) q.tenantId = tenantReq;
       const doc = await col.findOne(q, { projection: { _id: 0 } });

       if (doc) return res.json({ ok: true, ...doc });
     }
 
     // Fallback local solo para el CLIENT_ID actual (no podemos leer QR de otros procesos)
     if (cid === CLIENT_ID) {
        return res.json({ ok: true, tenantId: tenantReq || TENANT_ID, clientId: CLIENT_ID, telefono_qr, status: await safeGetClientState(), qrDataUrl: lastQrDataUrl, qrAt: lastQrAt ? lastQrAt.toISOString() : null, mongoReady });

     }
 
     return res.status(404).json({ ok: false, error: "No encontrado (sin Mongo no hay QR remoto)" });
   } catch (e) {
     return res.status(500).json({ ok: false, error: String(e?.message || e) });
   }
 });


RecuperarJsonConf();
//headless = devolver_headless();
// Si cliente.json trae puerto, lo aplicamos ANTES del listen
if (OVERRIDE_PORT) {
  port = parseInt(OVERRIDE_PORT, 10);
}

//const port =  devolver_puerto();
// Permitir override de headless por env (útil si lo controlás desde el sheet)
// Permitir override de headless por cliente.json
if (OVERRIDE_HEADLESS === "true" || OVERRIDE_HEADLESS === "1" || OVERRIDE_HEADLESS === "yes") headless = true;
if (OVERRIDE_HEADLESS === "false" || OVERRIDE_HEADLESS === "0" || OVERRIDE_HEADLESS === "no") headless = false;


server.listen(port, function() {
  console.log('App running on *: ' + port);
  EscribirLog('App running on *: ' + port,"event");

});

 
 // Intentamos conectar a Mongo en background (sin bloquear arranque)
 initMongoAndMaybeRestoreSession().catch(e => {
   EscribirLog("Mongo init error (se continua en local): " + String(e?.message || e), "error");
 });

const client = new Client({


  restartOnAuthFail: true,
  puppeteer: {
   headless: headless,
   ...(CHROME_PATH ? { executablePath: CHROME_PATH } : {}),
   
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu'
    ],
  },
  authStrategy: new LocalAuth({
    clientId: CLIENT_ID,
    dataPath: SESSION_PATH
  })
});
 
 // Helpers (no rompen si Mongo no está)
 async function safeGetClientState() {
   try {
     const st = await client.getState();
     return st || null;
   } catch (e) {
     return null;
   }
 }

/**
 * Envío robusto con reintentos ante errores de evaluación/recarga en WhatsApp Web
*/
async function safeSend(to, content, opts) {
  // Normaliza destino: si viene sin "@c.us" o "@g.us", asumimos contacto individual.
  let waTo = String(to ?? "");
  if (waTo && !waTo.includes("@")) waTo = waTo + "@c.us";

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      // Estado del cliente (CONNECTED/OPENING/etc.). Si falla, seguimos intentando.
      let state = null;
      try { state = await client.getState(); } catch (e) { /* ignore */ }
      if (state !== 'CONNECTED') await sleep(700 * attempt);

      // Ayuda a que WhatsApp Web tenga el chat "resuelto" (reduce undefined internos)
      try { await client.getChatById(waTo); } catch (e) { /* ignore */ }

      const res = await client.sendMessage(waTo, content, opts);
    EscribirLog(`safeSend OK attempt=${attempt} to=${waTo}`, "event");
      return res;
    } catch (e) {
      const msg = String(e && e.message ? e.message : e);
      const transient = msg.includes("Evaluation failed") ||
                        msg.includes("Execution context was destroyed") ||
                        msg.includes("Protocol error");

      // BUG típico: markedUnread. Reintenta con backoff en vez de crashear o saltear.
      if (msg.includes("markedUnread")) {
        EscribirLog(`WARN safeSend(): markedUnread attempt=${attempt} to=${waTo} -> ${msg}`, "error");
        await sleep(1200 * attempt);
        continue;
      }

      if (!transient || attempt === 3) {
        EscribirLog(`safeSend FAIL attempt=${attempt} to=${waTo} -> ${msg}`, "error");
        throw e;
      }
      await sleep(500 * attempt);
    }
  }

  EscribirLog(`safeSend GAVEUP to=${waTo}`, "error");
  return null;
}


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
                  await safeSend(Nro_tel_format,media,{caption:  Msj });
                  await io.emit('message', 'Mensaje: '+Nro_tel_format+': '+ Msj );
                  //await safeSend('5493462674128@c.us',media,{caption:  Msj });
                  
                  } else{
                    console.log("msj texto");
                    if (Msj == null){ Msj = ''}
                    await safeSend(Nro_tel_format,  Msj );
                    await io.emit('message', 'Mensaje: '+Nro_tel_format+': '+ Msj );
                    //await safeSend('5493462674128@c.us',  Msj );
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
        console.log("ApiWhatsapp - Response ERROR "+JSON.stringify(json));
        EscribirLog("ApiWhatsapp - Response ERROR "+JSON.stringify(json),"error");
       
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

 // var indice_telefono = indexOf2d(message.from);

 // Normalizamos a un destinatario estable para responder (preferimos @c.us)
  let replyTo = String(message.from || "");
  try {
    const fromId = String(message.from || "");
    const authorId = String(message.author || ""); // solo grupos
    const idToResolve = fromId.endsWith("@g.us") ? authorId : fromId;
    if (idToResolve) {
      const c = await client.getContactById(idToResolve);
      if (c && c.number) replyTo = `${c.number}@c.us`;
    }
  } catch (e) {
    // fallback: replyTo queda como message.from
  }

  // Usamos replyTo como "key" de conversación para que no cambie si WA manda @lid
  var indice_telefono = indexOf2d(replyTo);

 if(indice_telefono == -1){

  var valor_i=0;
 }else{
 var valor_i = jsonGlobal[indice_telefono][1];
 }
 
EscribirLog(message.from +' '+message.to+' '+message.type+' '+message.body ,"event");


  console.log("mensaje "+message.from);
 
  
//if (message.from=='5493462514448@c.us'   ){

  
    
  if( valor_i==0) {
    
    RecuperarJsonConfMensajes();
   
    var segundos = Math.random() * (devolver_seg_hasta() - devolver_seg_desde()) + devolver_seg_desde();

   // Para API: queremos SOLO números (sin @c.us / @lid / @g.us)
   var telefonoTo = String(message.to || "");

      // En grupos: message.from termina en @g.us y el autor real viene en message.author
      const fromId = String(message.from || "");
      const authorId = String(message.author || ""); // solo en grupos

      let telefonoFrom = null;
      try {
        // Si es grupo, intentamos resolver el autor; si no, resolvemos el from.
        const idToResolve = fromId.endsWith("@g.us") ? authorId : fromId;
        if (idToResolve) {
          const contact = await client.getContactById(idToResolve);
          console.log(contact?.id?._serialized);
          console.log(contact?.number);
          telefonoFrom = contact?.number || null;
        }
      } catch (e) {
        telefonoFrom = null;
      }
    //var telefonoFrom = '5493425472992@c.us' 
   // var telefonoTo = '5493424293943@c.us'
//telefonoTo = telefonoTo.replace('@c.us','').replace('@lid','');
//telefonoFrom = telefonoFrom.replace('@c.us','').replace('@lid','');
      // Normalizar destino/origen para API (solo números)
      telefonoTo = telefonoTo.replace('@c.us','').replace('@lid','').replace('@g.us','');

      // Si no pudimos obtener number, fallback al id (author o from)
      if (!telefonoFrom) {
        const fallback = fromId.endsWith("@g.us") ? authorId : fromId;
        telefonoFrom = String(fallback || "").replace('@c.us','').replace('@lid','').replace('@g.us','');
      } else {
        telefonoFrom = String(telefonoFrom); // ya viene sin sufijos
      }


   
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
        await safeSend(replyTo, msg_inicio);
      }

      await io.emit('message', 'Mensaje: '+telefonoFrom+': '+ message.body );

       var jsonTexto = {
        Tel_Origen: String(telefonoFrom || "").replace('@c.us','').replace('@lid',''),
        Tel_Destino: String(telefonoTo || "").replace('@c.us','').replace('@lid',''),
        Mensaje: message?.body ?? "",
        Respuesta: ""
      };

      //var jsonTexto = {  Tel_Origen: telefonoFrom ?? "",  Tel_Destino: telefonoTo ?? "",  Mensaje: message?.body ?? "",  Respuesta: ""};
   
      //jsonTexto = {Tel_Origen:telefonoFrom,Tel_Destino:telefonoTo, Mensaje:message.body,Respuesta:''};
      // jsonTexto = {Tel_Origen:'5493462674128',Tel_Destino:'5493424293943', Mensaje:message.body,Respuesta:''};

      let url =  api

      console.log(JSON.stringify(jsonTexto));
      EscribirLog("Mensaje "+JSON.stringify(jsonTexto),'event');
      
   let controller; let timeoutId;
   try{
         controller = new AbortController();
         timeoutId = setTimeout(() => controller.abort(), 55000);
 
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

       
         console.log(json)
         
         if (!resp.ok) {
            const detalle = json ? JSON.stringify(json) : raw;
             EscribirLog("Error 02 ApiWhatsapp - Response ERROR " + detalle, "error");
             EnviarEmail("ApiWhatsapp - Response ERROR ", detalle);
             if (msg_errores) await safeSend(replyTo, msg_errores);
            return "error";
          }

             tam_json = 0;
            // Guardamos el estado por replyTo (estable)
            recuperar_json(replyTo, json);
            await procesar_mensaje(json, message, replyTo);

            if (msg_fin) {
               await safeSend(replyTo, msg_fin);
            }



           return "ok";

} catch (err) {
  clearTimeout(timeoutId);
  // Importante: no dependas de jsonTexto indefinido en el log
  const detalle = "Error 03 Chatbot Error " + (err?.message || err) + " " + JSON.stringify(jsonTexto);
  console.log(detalle);
  EscribirLog(detalle, "error");
  EnviarEmail("Chatbot Error ", detalle);
  if (msg_errores) await safeSend(replyTo, msg_errores);
  return "error";
}





////////////////////
    };
   
    var body = message.body;
    body = body.trim();
    body = body.toUpperCase();


    if(valor_i !== 0 && body == 'N' ){
      console.log("cancelar"&msg_can);
      //safeSend(message.from,'*Consulta Cancelada* ❌' );
            
      if(msg_can == '' || msg_can == undefined || msg_can == 0){
        
        
      }else{
        await safeSend(replyTo, msg_can);

      }
      bandera_msg=1;
      jsonGlobal[indice_telefono][2] = '';
      jsonGlobal[indice_telefono][1] = 0;
      jsonGlobal[indice_telefono][3] = '';

    
    };
    if(valor_i!==0 && ((body != 'N') && (body != 'S' ) )){
      console.log("no entiendo ->"+message.body);
      await safeSend(replyTo,'🤔 *No entiendo*, \nPor favor ingrese *S* o *N* para mostrar los siguientes resultados\n ' );

    };
    

    if(valor_i !== 0 && body == 'S'){
      console.log("continuar "+tam_json+' indice '+indice_telefono);
      procesar_mensaje(jsonGlobal[indice_telefono][2], message);

     }
//}  //

});






/*client.on('message_ack', (message2, ack) => {


  console.log('Mensaje ' + message2.id.id);
  console.log('Estado ' + ack);
  console.log('id_msg '+Id_msj_dest+'  id_renlon '+Id_msj_renglon);
});
*/

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
client.initialize();

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////
/// ENVIO DE LOG A APLICACION CLIENTE
//////////////////////////////////////////////////////////////
// Socket IO
/*io.on('connection', async function(socket) {
 
  socket.emit('message', 'Conectando...');

  client.on('qr', (qr) => {
    console.log('QR RECEIVED', qr);
    qrcode.toDataURL(qr, (err, url) => {
    socket.emit('qr', url);
    socket.emit('message', 'Código QR Recibido...');
    });
  });

  client.on('ready', async () => {
   // console.log("listo...");
    //controlar_hora_msg();
  //  socket.emit('ready', 'Whatsapp Listo!');
    socket.emit('message', 'Whatsapp listo!');
    
  });

  client.on('authenticated', async () => {
    socket.emit('authenticated', 'Whatsapp Autenticado!.');
    socket.emit('message', 'Whatsapp Autenticado!');
    console.log('Autenticado');

  });



  client.on('auth_failure', function(session) {
    socket.emit('message', 'Auth failure, restarting...');
    chatbot.EnviarEmail('Chatbot error Auth failure','Auth failure, restarting...'+client);
  });

  client.on('disconnected', (reason) => {
    socket.emit('message', 'Whatsapp Desconectado!');
    chatbot.EnviarEmail('Chatbot Desconectado ','Desconectando...'+client);
    client.destroy();
    client.initialize();
  });
});

*/


client.on('ready', async () => {
  console.log("listo ready....");
  telefono_qr = client.info.me.user
  console.log("TEL QR: "+client.info.me.user);
  
    
   await io.emit('message', 'Whatsapp Listo!');
   EscribirLog('Whatsapp Listo!',"event");

 
     // Persistimos estado en Mongo (si está)
     upsertSessionState({
       clientId: CLIENT_ID,
       telefono_qr,
       status: "ready"
     }).catch(() => {});
 
     // Backup de sesión LocalAuth a Mongo (si está)
     backupLocalAuthToMongo().catch(() => {});

  //ConsultaApiMensajes();


});

client.on('qr', (qr) => {
  console.log('QR RECEIVED 3.00.01', qr);
 
   // Guardamos QR en memoria para panel
   lastQrRaw = qr;
   lastQrAt = new Date();
 

  qrcode.toDataURL(qr, (err, url) => {
    lastQrDataUrl = url || null;
  io.emit('qr', url);
  io.emit('message', 'Código QR Recibido...');
 
   // Persistimos QR en Mongo para el panel (si está)
   upsertSessionState({
     clientId: CLIENT_ID,
     telefono_qr,
     status: "qr",
     qrDataUrl: lastQrDataUrl,
     qrAt: lastQrAt
   }).catch(() => {});
  });
});


client.on('authenticated', async () => {
  io.emit('authenticated', 'Whatsapp Autenticado!.');
  io.emit('message', 'Whatsapp Autenticado!');
  console.log('Autenticado');
  EscribirLog('Autenticado',"event");
 
   // Persistimos estado en Mongo (si está)
   upsertSessionState({
     clientId: CLIENT_ID,
     telefono_qr,
     status: "authenticated"
   }).catch(() => {});
 
   // Backup de sesión LocalAuth a Mongo (si está)
   backupLocalAuthToMongo().catch(() => {});
});



client.on('auth_failure', function(session) {
  io.emit('message', 'Auth failure, restarting...');
  EnviarEmail('Chatbot error Auth failure','Auth failure, restarting...'+client);
  EscribirLog('Error 04 - Chatbot error Auth failure','Auth failure, restarting...',"error");
 
   upsertSessionState({
     clientId: CLIENT_ID,
     telefono_qr,
     status: "auth_failure"
   }).catch(() => {});
});

client.on('disconnected', (reason) => {
  io.emit('message', 'Whatsapp Desconectado!');
  EnviarEmail('Chatbot Desconectado ','Desconectando...'+client);
  EscribirLog('Chatbot Desconectado ','Desconectando...',"event");
 
   upsertSessionState({
     clientId: CLIENT_ID,
     telefono_qr,
     status: "disconnected",
     reason: String(reason || "")
   }).catch(() => {});
 
  client.destroy();
  client.initialize();
});
 
 //////////////////////////////////////////////////////////////////////////////////////////////////
 // MongoDB - implementación (opcional) con fallback local
 //////////////////////////////////////////////////////////////////////////////////////////////////
 async function initMongoAndMaybeRestoreSession() {
   if (!MONGO_URI) {
     mongoReady = false;
     EscribirLog("Mongo: MONGO_URI vacío, se trabaja en local.", "event");
     return;
   }
   if (!MongoClient || !GridFSBucket) {
     mongoReady = false;
     EscribirLog("Mongo: dependencia 'mongodb' no disponible, se trabaja en local.", "error");
     return;
   }
 
   try {
     mongoClient = new MongoClient(MONGO_URI, {
       serverSelectionTimeoutMS: MONGO_CONNECT_TIMEOUT_MS
     });
     await mongoClient.connect();
     mongoDb = mongoClient.db(MONGO_DB || undefined);
     gridfsBucket = new GridFSBucket(mongoDb, { bucketName: MONGO_BUCKET });
     mongoReady = true;
     EscribirLog("Mongo: conectado OK (sesiones habilitadas).", "event");
 
     // Restore de LocalAuth desde Mongo (si falta local)
     await restoreLocalAuthFromMongoIfNeeded();
 
     // Upsert inicial de estado
     await upsertSessionState({
       clientId: CLIENT_ID,
       telefono_qr,
       status: "boot"
     });
   } catch (e) {
     mongoReady = false;
    mongoClient = null;
     mongoDb = null;
     gridfsBucket = null;
     EscribirLog("Mongo: NO conecta, se trabaja en local. Detalle: " + String(e?.message || e), "error");
   }
 }
 
 async function upsertSessionState(payload) {
   try {
     if (!mongoReady || !mongoDb) return;
     const col = mongoDb.collection(MONGO_SESSIONS_COLLECTION);
     const now = new Date();
     const doc = {
      tenantId: String(payload.tenantId || TENANT_ID || "").trim() || TENANT_ID,
       clientId: payload.clientId || CLIENT_ID,
       telefono_qr: payload.telefono_qr || telefono_qr || null,
       status: payload.status || null,
       reason: payload.reason || null,
       qrDataUrl: payload.qrDataUrl ?? lastQrDataUrl ?? null,
       qrAt: payload.qrAt ? new Date(payload.qrAt) : (lastQrAt ? new Date(lastQrAt) : null),
       mongoReady: true,
       updatedAt: now
     };
     await col.updateOne({ tenantId: doc.tenantId, clientId: doc.clientId }, { $set: doc }, { upsert: true });
   } catch (e) {
     // si falla, no cortamos el flujo principal
     EscribirLog("Mongo upsertSessionState error: " + String(e?.message || e), "error");
   }
 }
 
 function localAuthSessionDir() {
   // whatsapp-web.js LocalAuth crea "session-<clientId>" dentro de dataPath
   return path.join(SESSION_PATH, `session-${CLIENT_ID}`);
 }
 
 async function restoreLocalAuthFromMongoIfNeeded() {
   try {
     if (!mongoReady || !mongoDb || !gridfsBucket) return;
     if (!AdmZip) {
       EscribirLog("Mongo restore: falta 'adm-zip' (se omite restore).", "error");
       return;
     }
 
    const sessDir = localAuthSessionDir();
     const exists = fs.existsSync(sessDir);
     const notEmpty = exists && fs.readdirSync(sessDir).length > 0;
     if (notEmpty) {
       EscribirLog("Mongo restore: existe sesión local, no restaura.", "event");
       return;
     }
 
     // Buscar el zip en GridFS
     const filename = `localauth-${TENANT_ID}-${CLIENT_ID}.zip`;
     const files = await mongoDb.collection(`${MONGO_BUCKET}.files`).find({ filename }).sort({ uploadDate: -1 }).limit(1).toArray();
     if (!files || files.length === 0) {
       EscribirLog("Mongo restore: no hay backup en Mongo para " + filename, "event");
       return;
     }
 
     // Descargar a buffer
     const fileId = files[0]._id;
     const chunks = [];
     await new Promise((resolve, reject) => {
       gridfsBucket.openDownloadStream(fileId)
         .on("data", (d) => chunks.push(d))
         .on("error", reject)
         .on("end", resolve);
     });
     const buf = Buffer.concat(chunks);

     // Extraer en SESSION_PATH
     fs.mkdirSync(SESSION_PATH, { recursive: true });
     const zip = new AdmZip(buf);
     zip.extractAllTo(SESSION_PATH, true);
     EscribirLog("Mongo restore: sesión restaurada a " + SESSION_PATH, "event");
   } catch (e) {
     EscribirLog("Mongo restore error (se continua en local): " + String(e?.message || e), "error");
   }
 }
 

 async function backupLocalAuthToMongo() {
   try {
     if (!mongoReady || !mongoDb || !gridfsBucket) return;
     if (!AdmZip) {
       // No es fatal: solo omite backup
       return;
     }
 
     const sessDir = localAuthSessionDir();
     if (!fs.existsSync(sessDir)) return;
 
     // Armamos zip con todo SESSION_PATH para mantener estructura (incluye session-CLIENT_ID)
     const zip = new AdmZip();
     zip.addLocalFolder(SESSION_PATH);
     const buf = zip.toBuffer();
 
      const filename = `localauth-${TENANT_ID}-${CLIENT_ID}.zip`;
 
     // Borramos backups anteriores con mismo filename (si existen)
     const oldFiles = await mongoDb.collection(`${MONGO_BUCKET}.files`).find({ filename }).toArray();
     for (const f of oldFiles) {
       try { await gridfsBucket.delete(f._id); } catch (e) { /* ignore */ }
     }
 
     await new Promise((resolve, reject) => {
       const up = gridfsBucket.openUploadStream(filename, {
         metadata: {
           clientId: CLIENT_ID,
           telefono_qr: telefono_qr || null,
           createdAt: new Date()
         },
         contentType: "application/zip"
       });
       up.on("error", reject);
       up.on("finish", resolve);
       up.end(buf);
     });
 
     EscribirLog("Mongo backup: sesión guardada en GridFS (" + filename + ")", "event");
   } catch (e) {
     EscribirLog("Mongo backup error (se continua en local): " + String(e?.message || e), "error");
   }
 }



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

async function procesar_mensaje(json, message, replyToOverride){
  
  RecuperarJsonConfMensajes();

  // Usamos la misma key estable que usa el handler
  const replyTo = replyToOverride || String(message.from || "");
  var indice = indexOf2d(replyTo);
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
        
         await safeSend(replyTo, mensaje);
         await sleep(segundos);
         await io.emit('message', 'Respuesta: '+replyTo+': '+ mensaje );
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
          await safeSend(replyTo, msg_loc);
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
            safeSend(jsonGlobal[i][0],msg_cad );

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

  // Todo se lee de cliente.json (que contiene "configuracion")
  let rawCliente = null;
  let conf = null;
  try {
    rawCliente = JSON.parse(fs.readFileSync('cliente.json', 'utf8'));
    conf = (rawCliente && rawCliente.configuracion && typeof rawCliente.configuracion === "object")
      ? rawCliente.configuracion
      : rawCliente;
  } catch (e) {
    // si no hay cliente.json, no rompemos (quedan defaults)
    return;
  }

  // Config "mensajes"
  if (conf) {
    if (conf.seg_desde != null) seg_desde = conf.seg_desde;
    if (conf.seg_hasta != null) seg_hasta = conf.seg_hasta;
    if (conf.dsn != null) dsn = conf.dsn;
    if (conf.seg_msg != null) seg_msg = conf.seg_msg;
    if (conf.api != null) api = conf.api;
    if (conf.msg_inicio != null) msg_inicio = conf.msg_inicio;
    if (conf.msg_fin != null) msg_fin = conf.msg_fin;
    if (conf.cant_lim != null) cant_lim = conf.cant_lim;
    if (conf.time_cad != null) time_cad = conf.time_cad;
    if (conf.msg_lim != null) msg_lim = conf.msg_lim;
    if (conf.msg_cad != null) msg_cad = conf.msg_cad;
    if (conf.msg_can != null) msg_can = conf.msg_can;
    if (conf.nom_emp != null) nom_chatbot = conf.nom_emp;
    if (conf.email_err != null) email_err = conf.email_err;
    if (conf.seg_tele != null) seg_tele = conf.seg_tele;
  }

  // SMTP/errores: primero intentamos desde cliente.json (si lo incluiste ahí),
  // y si NO existe, mantenemos compat con configuracion_errores.json (por si todavía lo usás).
  try {
    const errBlock = (rawCliente && (rawCliente.errores || rawCliente.configuracion_errores || rawCliente.email || rawCliente.smtp))
      ? (rawCliente.errores || rawCliente.configuracion_errores || rawCliente)
      : null;

    if (errBlock) {
      if (errBlock.smtp != null) smtp = errBlock.smtp;
      if (errBlock.user != null) email_usuario = errBlock.user;
      if (errBlock.pass != null) email_pas = errBlock.pass;
      if (errBlock.puerto != null) email_puerto = errBlock.puerto;
      if (errBlock.email_sal != null) email_saliente = errBlock.email_sal;
      if (errBlock.msg_error != null) msg_errores = errBlock.msg_error;
      if (errBlock.email_err != null) email_err = errBlock.email_err;
    } else {
      // Compat LEGACY (no obligatorio)
      const jsonError = JSON.parse(fs.readFileSync('configuracion_errores.json'));
      if (jsonError && jsonError.configuracion) {
        if (jsonError.configuracion.email_err != null) email_err = jsonError.configuracion.email_err;
        if (jsonError.configuracion.smtp != null) smtp = jsonError.configuracion.smtp;
        if (jsonError.configuracion.user != null) email_usuario = jsonError.configuracion.user;
        if (jsonError.configuracion.pass != null) email_pas = jsonError.configuracion.pass;
        if (jsonError.configuracion.puerto != null) email_puerto = jsonError.configuracion.puerto;
        if (jsonError.configuracion.email_sal != null) email_saliente = jsonError.configuracion.email_sal;
        if (jsonError.configuracion.msg_error != null) msg_errores = jsonError.configuracion.msg_error;
      }
    }
  } catch (e) {}

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
  
   // Todo se lee de cliente.json (que contiene "configuracion")
  let rawCliente = null;
  let conf = null;
  try {
    rawCliente = JSON.parse(fs.readFileSync('cliente.json', 'utf8'));
    conf = (rawCliente && rawCliente.configuracion && typeof rawCliente.configuracion === "object")
      ? rawCliente.configuracion
      : rawCliente;
  } catch (e) {
    // si no hay cliente.json, no rompemos (quedan defaults)
    return;
  }

  console.log("cliente.json configuracion "+(conf || {}));

  if (conf) {
    if (conf.puerto != null) port = conf.puerto;
    console.log("puerto: "+port);

    if (conf.headless != null) headless = conf.headless;
    console.log("headless: "+headless);

    if (conf.seg_desde != null) seg_desde = conf.seg_desde;
    console.log("seg_desde: "+seg_desde);

    if (conf.seg_hasta != null) seg_hasta = conf.seg_hasta;
    console.log("seg_hasta: "+seg_hasta);

    if (conf.dsn != null) dsn = conf.dsn;
    console.log("dsn: "+dsn);

    if (conf.seg_msg != null) seg_msg = conf.seg_msg;
    console.log("seg_msg: "+seg_msg);

    if (conf.seg_tele != null) seg_tele = conf.seg_tele;
    console.log("seg_tele: "+seg_tele);

    if (conf.api != null) api = conf.api;
    console.log("api: "+api);

    if (conf.msg_inicio != null) msg_inicio = conf.msg_inicio;
    console.log("msg_inicio: "+msg_inicio);

    if (conf.msg_fin != null) msg_fin = conf.msg_fin;
    console.log("msg_fin: "+msg_fin);

    if (conf.nom_emp != null) nom_chatbot = conf.nom_emp;
    if (conf.email_err != null) email_err = conf.email_err;
  }

  // Normaliza headless (viene "true"/"false" como string en tu config completa) :contentReference[oaicite:2]{index=2}
  if (headless == 'true') headless = true;
  else if (headless == 'false') headless = false;

   // --- Overrides por cliente.json (si existen) ---
   if (OVERRIDE_PORT) port = parseInt(OVERRIDE_PORT, 10);
   if (OVERRIDE_HEADLESS === "true" || OVERRIDE_HEADLESS === "1" || OVERRIDE_HEADLESS === "yes") headless = true;
   if (OVERRIDE_HEADLESS === "false" || OVERRIDE_HEADLESS === "0" || OVERRIDE_HEADLESS === "no") headless = false;
   if (OVERRIDE_API_URL) api = OVERRIDE_API_URL;

}



// ISO-like: 2025-09-04T13:45:22 (hora de Argentina)
function nowArgentinaISO() {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: AR_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(new Date()); // "YYYY-MM-DD HH:mm:ss"
  return s.replace(' ', 'T'); // "YYYY-MM-DDTHH:mm:ss"
}

// (Opcional) con offset: 2025-09-04T13:45:22-03:00
function nowArgentinaISOWithOffset() {
  const d = new Date();
  const base = new Intl.DateTimeFormat('sv-SE', {
    timeZone: AR_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(d).replace(' ', 'T');

  // Si tu versión de Node soporta 'longOffset', obtenemos "-03:00"
  const tzName = new Intl.DateTimeFormat('en', {
    timeZone: AR_TZ,
    timeZoneName: 'longOffset' // devuelve "GMT-03:00" en Node moderno
  }).formatToParts(d).find(p => p.type === 'timeZoneName')?.value;

  const offset = tzName ? tzName.replace('GMT', '') : '-03:00';
  return base + offset;
}

function EscribirLog(mensaje, tipo) {
  const timestamp = nowArgentinaISO(); // o nowArgentinaISOWithOffset()
  const logMessage = `[${timestamp}] ${mensaje}\n`;
  const file = (tipo === 'event') ? logFilePath_event : logFilePath_error;

  fs.appendFile(file, logMessage, (err) => {
    if (err) {
      console.error('Error al escribir en el archivo de log:', err);
    }
  });
}
