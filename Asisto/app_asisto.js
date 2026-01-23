/*script:app_asisto*/
/*version:2.00.10   23/01/2026*/

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
// Overrides por cliente (ENV)
// -------------------------------
const CLIENT_ID = process.env.CLIENT_ID || "default";
const SESSION_PATH = process.env.SESSION_PATH || path.join(__dirname, ".wwebjs_auth");
const CHROME_PATH = process.env.CHROME_PATH || ""; // opcional

const ENV_PORT = process.env.PORT || "";
const ENV_HEADLESS = (process.env.HEADLESS || "").toLowerCase();
const ENV_API_URL = process.env.API_URL || "";


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


app.use(fileUpload({
  debug: false
}));

app.get('/', (req, res) => {
  res.sendFile('index.html', {
    root: __dirname
  });
});

RecuperarJsonConf();
//headless = devolver_headless();

//const port =  devolver_puerto();
// Permitir override de headless por env (útil si lo controlás desde el sheet)
if (ENV_HEADLESS === "true" || ENV_HEADLESS === "1" || ENV_HEADLESS === "yes") headless = true;
if (ENV_HEADLESS === "false" || ENV_HEADLESS === "0" || ENV_HEADLESS === "no") headless = false;


server.listen(port, function() {
  console.log('App running on *: ' + port);
  EscribirLog('App running on *: ' + port,"event");

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

  //ConsultaApiMensajes();


});

client.on('qr', (qr) => {
  console.log('QR RECEIVED 2.00.09', qr);
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



client.on('auth_failure', function(session) {
  io.emit('message', 'Auth failure, restarting...');
  EnviarEmail('Chatbot error Auth failure','Auth failure, restarting...'+client);
  EscribirLog('Error 04 - Chatbot error Auth failure','Auth failure, restarting...',"error");
});

client.on('disconnected', (reason) => {
  io.emit('message', 'Whatsapp Desconectado!');
  EnviarEmail('Chatbot Desconectado ','Desconectando...'+client);
  EscribirLog('Chatbot Desconectado ','Desconectando...',"event");
  client.destroy();
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

  const jsonConf =  JSON.parse(fs.readFileSync('configuracion.json'));
  const jsonError = JSON.parse(fs.readFileSync('configuracion_errores.json'));
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

   // --- Overrides por ENV (si existen) ---
   if (process.env.SMTP_HOST) smtp = process.env.SMTP_HOST;
   if (process.env.SMTP_USER) email_usuario = process.env.SMTP_USER;
   if (process.env.SMTP_PASS) email_pas = process.env.SMTP_PASS;
   if (process.env.SMTP_PORT) email_puerto = parseInt(process.env.SMTP_PORT, 10);
   if (process.env.EMAIL_SALIENTE) email_saliente = process.env.EMAIL_SALIENTE;
   if (process.env.EMAIL_ENVIO) email_err = process.env.EMAIL_ENVIO;

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
  
  const jsonConf =  JSON.parse(fs.readFileSync('configuracion.json'));
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

   // --- Overrides por ENV (si existen) ---
   if (ENV_PORT) port = parseInt(ENV_PORT, 10);
   if (ENV_HEADLESS === "true" || ENV_HEADLESS === "1" || ENV_HEADLESS === "yes") headless = true;
   if (ENV_HEADLESS === "false" || ENV_HEADLESS === "0" || ENV_HEADLESS === "no") headless = false;
   if (ENV_API_URL) api = ENV_API_URL;
   // server.listen(port, function() {
  //console.log('App running on *: ' + port);
//});

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
