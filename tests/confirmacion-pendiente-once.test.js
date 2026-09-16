const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'app_asisto_ws.js'), 'utf8');
const start = source.indexOf('async function procesarPendientesDocConfirmacionApiMensajes(');
const end = source.indexOf('async function procesarPendientesConfirmacionApiMensajes(', start);
assert.ok(start >= 0 && end > start);

let claimed = false;
let sent = 0;
let completed = false;
const col = {
  async updateOne(query, update) {
    if (query['pendientes.551_707.envioClaimedAt']) {
      if (claimed || completed) return { matchedCount: 0 };
      claimed = true;
      return { matchedCount: 1 };
    }
    if (update.$unset) completed = true;
    return { matchedCount: 1 };
  }
};
const context = {
  apiMensajesConfirmacionCollection: () => col,
  pendientesConfirmacionApiMensajesArray: (doc) => [doc.pendientes['551_707']],
  buildUrlConfirmaApiMensajes: () => 'unused',
  onlyDigits: (value) => String(value).replace(/\D/g, ''),
  keyPendienteConfirmacionApiMensajes: () => '551_707',
  pendienteYaRegistradoComoEnviadoApiMensajes: async () => completed,
  detectMimeType: () => 'image/png',
  mime: { lookup: () => 'image/png' },
  MessageMedia: class {},
  io: { emit: () => {} },
  enviarApiMensajesConPausa: async (_to, callback) => callback(),
  safeSend: async () => {
    sent++;
    await new Promise((resolve) => setTimeout(resolve, 15));
    return { id: { _serialized: 'test' } };
  },
  recordApiMensajesBillingWindow: async () => {},
  marcarPendienteEnviadoApiMensajes: async () => { completed = true; return true; },
  getInfoContactoApiMensajes: async () => ({}),
  actualizarEstadoUnidadApiMensajes: async () => true,
  EscribirLog: () => {},
  console: { log: () => {} },
  apiMensajesFallosConsecutivos: 0
};
vm.runInNewContext(source.slice(start, end) + '\nthis.procesar = procesarPendientesDocConfirmacionApiMensajes;', context);

const doc = {
  _id: 'SDG:5493462514448:5493462621586',
  estado: 'aceptado',
  nroTel: '5493462621586',
  pendientes: {
    '551_707': {
      key: '551_707', nroTel: '5493462621586',
      id_msj_dest: 551, id_msj_renglon: 707,
      msj: 'Prueba', content: 'AAAA', content_nombre: '1.PNG'
    }
  }
};

Promise.all([context.procesar(doc, 'E', 'ok'), context.procesar(doc, 'E', 'ok')])
  .then(async () => {
    assert.equal(sent, 1, 'dos procesadores concurrentes no deben enviar dos imágenes');
    completed = false;
    doc.pendientes['551_707'].envioClaimedAt = new Date();
    const result = await context.procesar(doc, 'E', 'recovery');
    assert.equal(sent, 1, 'un envío incierto no se debe repetir automáticamente');
    assert.equal(result.errores[0].error, 'envio_incierto_revisar');
  })
  .catch((error) => { console.error(error); process.exitCode = 1; });
