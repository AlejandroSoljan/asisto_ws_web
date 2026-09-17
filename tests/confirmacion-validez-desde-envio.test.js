const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'app_asisto_ws.js'), 'utf8');
function extract(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start);
  assert.ok(start >= 0 && end > start);
  return source.slice(start, end);
}
const implementation = [
  extract('function apiMensajesConfirmacionSolicitudVigente(', 'function getWhatsappMessageTimestampMs('),
  extract('async function estadoConfirmacionApiMensajes(', 'async function registrarRespuestaConfirmacionApiMensajes('),
].join('\n');

function setup(doc, validezMs = 86400000, tenantId = 'RVL') {
  let sent = 0;
  const updates = [];
  const col = {
    findOne: async () => doc,
    updateOne: async (_query, update) => { updates.push(update); return { matchedCount: 1 }; },
  };
  const context = {
    tenantId,
    api_mensajes_confirmacion_habilitada: true,
    api_mensajes_requerir_contacto_o_historial: false,
    api_mensajes_confirmacion_validez_ms: validezMs,
    api_mensajes_confirmacion_reenviar_ms: 259200000,
    requiereConfirmacionPrioridadApiMensajes: () => true,
    onlyDigits: value => String(value).replace(/\D/g, ''),
    ensureMongo: async () => true,
    apiMensajesConfirmacionCollection: () => col,
    apiMensajesConfirmacionId: to => to,
    arDatePartsForStats: () => ({ dayKey: '2026-09-17' }),
    estadoLimiteDiarioApiMensajes: async () => ({ permitido: true }),
    señalesContactoApiMensajes: async () => ({ contactoConocido: true, esContactoPropio: true }),
    textoSolicitudConfirmacionApiMensajes: () => 'Confirmación de prueba',
    enviarApiMensajesConPausa: async (_phone, send) => send(),
    safeSend: async () => { sent++; return { id: 'sent' }; },
    recordApiMensajesBillingWindow: async () => {},
    respuestasOkApiMensajesConfirmacion: () => ['OK'],
    getApiMensajesNroTelFrom: () => '5493462239802',
    detectarNoValidaConfirmacionApiMensajesEnChat: async () => false,
    detectarOkConfirmacionApiMensajesEnChat: async () => false,
    console: { log() {} },
    EscribirLog() {},
    Date,
    Number,
    String,
  };
  vm.runInNewContext(implementation + '\nthis.check = estadoConfirmacionApiMensajes;', context);
  return { check: context.check, get sent() { return sent; }, updates };
}

const hoursAgo = hours => new Date(Date.now() - hours * 3600000);

test('aceptó: no vuelve a pedir antes de 24 h contadas desde el envío', async () => {
  const state = setup({ estado: 'aceptado', pedidoAt: hoursAgo(23), aceptadoAt: hoursAgo(1) });
  assert.equal((await state.check('5491111111111', 'fc a 123')).autorizado, true);
  assert.equal(state.sent, 0);
});

test('aceptó hace una hora, pero la solicitud salió hace 25 h: vuelve a pedir', async () => {
  const state = setup({ estado: 'aceptado', pedidoAt: hoursAgo(25), aceptadoAt: hoursAgo(1) });
  assert.equal((await state.check('5491111111111', 'fc a 456')).motivo, 'solicitud_enviada');
  assert.equal(state.sent, 1);
});

test('sin respuesta: no repite a las 23 h y sí ante otro mensaje a las 25 h', async () => {
  const vigente = setup({ estado: 'pendiente', pedidoAt: hoursAgo(23) });
  assert.equal((await vigente.check('5491111111111', 'fc a 789')).motivo, 'pendiente');
  assert.equal(vigente.sent, 0);
  const vencida = setup({ estado: 'pendiente', pedidoAt: hoursAgo(25) });
  assert.equal((await vencida.check('5491111111111', 'fc a 790')).motivo, 'solicitud_enviada');
  assert.equal(vencida.sent, 1);
});

test('cruzar de día calendario no adelanta el plazo de 24 h', async () => {
  const state = setup({ estado: 'aceptado', pedidoAt: hoursAgo(2), aceptadoAt: hoursAgo(1), solicitudDayKey: '2026-09-16' });
  assert.equal((await state.check('5491111111111', 'fc a 791')).autorizado, true);
  assert.equal(state.sent, 0);
});

test('cancelación temporal vencida no agrega otro plazo de espera', async () => {
  const state = setup({ estado: 'cancelado', pedidoAt: hoursAgo(25), canceladoAt: hoursAgo(1) });
  assert.equal((await state.check('5491111111111', 'fc a 999')).motivo, 'solicitud_enviada');
  assert.equal(state.sent, 1);
});

test('CANCELAR permanente sigue bloqueando nuevos pedidos', async () => {
  const state = setup({ estado: 'cancelado', exclusionPermanente: true, exclusionMotivo: 'cancelar_cliente', pedidoAt: hoursAgo(25) });
  assert.equal((await state.check('5491111111111', 'fc a 1001')).cancelarMensaje, true);
  assert.equal(state.sent, 0);
});

test('la misma regla del parámetro funciona también para NEA', async () => {
  const state = setup({ estado: 'aceptado', pedidoAt: hoursAgo(25), aceptadoAt: hoursAgo(1) }, 86400000, 'NEA');
  assert.equal((await state.check('5491111111111', 'Mensaje informativo')).motivo, 'solicitud_enviada');
  assert.equal(state.sent, 1);
});
