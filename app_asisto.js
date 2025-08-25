try {
  fs.mkdirSync(path.join(__dirname, 'sessions'), { recursive: true });
} catch (e) {
  console.error('No se pudo crear carpeta de sesión:', e.message);
}

// ---- Puppeteer Chrome resolver (instala en runtime si falta) ----
async function resolveChromeExecutable() {
  try {
    const ep = puppeteer.executablePath();
    if (ep && fs.existsSync(ep)) {
      console.log('Chromium encontrado en:', ep);
      return ep;
    }
  } catch (e) {
    console.log('No se pudo resolver executablePath inicial:', e.message);
  }
  const cacheDir = process.env.PUPPETEER_CACHE_DIR || '/opt/render/.cache/puppeteer';
  try {
    console.log('Intentando instalar Chrome en runtime...');
    await puppeteer.browsers.install({ browser: 'chrome', cacheDir });
  } catch (e) {
    console.error('Fallo al instalar Chrome en runtime:', e.message);
  }
  try {
    const ep2 = puppeteer.executablePath();
    console.log('Chromium tras instalación en:', ep2);
    return ep2;
  } catch (e) {
    console.error('No se pudo obtener executablePath luego de instalar:', e.message);
    return undefined;
  }
}

/*script:app_asisto*/
/*version:1.06.02   11/07/2025*/

//const chatbot = require("./funciones_asisto.js")

const puppeteer = require('puppeteer');const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');
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

server.listen(port, function() {
  console.log('App running on *: ' + port);
  EscribirLog('App running on *: ' + port,"event");

});


let client;
(async () => {
  const chromePath = await resolveChromeExecutable();
  client = new Client({
     restartOnAuthFail: true,
  puppeteer: {
   headless: headless,
   
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process', // <- this one doesn't works in Windows
      '--disable-gpu'
    ],
  },
  authStrategy: new LocalAuth()
 ,
    puppeteer: Object.assign({}, ({  restartOnAuthFail: true,
  puppeteer: {
   headless: headless,
   
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process', // <- this one doesn't works in Windows
      '--disable-gpu'
    ],
  },
  authStrategy: new LocalAuth()
  }).puppeteer || {}, {
      headless: 'new',
      executablePath: chromePath,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
        '--disable-gpu',
        '--disable-extensions',
        '--disable-features=VizDisplayCompositor',
        '--js-flags=--jitless'
      ]
    })
  });
  // Inicializar después de configurar
  try { // client.initialize() movido al bootstrap } catch(e) { console.error('Error en client.initialize:', e.message); }
})();


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

  var indice_telefono = indexOf2d(message.from);

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
        try { json = raw ? JSON.parse(raw) : null; } catch { /* no es JSON */ }

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
// }  //

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
  console.log('QR RECEIVED', qr);
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

   // server.listen(port, function() {
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
