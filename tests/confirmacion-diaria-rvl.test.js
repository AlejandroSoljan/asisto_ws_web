const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'app_asisto_ws.js'), 'utf8');
const start = source.indexOf('async function estadoConfirmacionApiMensajes(');
const end = source.indexOf('async function registrarRespuestaConfirmacionApiMensajes(', start);
assert.ok(start >= 0 && end > start);
const implementation = source.slice(start, end);

function setup(tenantId, doc) {
  let sent = 0;
  const updates = [];
  const col = { findOne: async () => doc, updateOne: async (_query, update) => { updates.push(update); return { matchedCount: 1 }; } };
  const context = {
    tenantId,
    api_mensajes_confirmacion_habilitada: true,
    api_mensajes_confirmacion_reenviar_ms: 259200000,
    api_mensajes_requerir_contacto_o_historial: false,
    requiereConfirmacionPrioridadApiMensajes: () => false,
    onlyDigits: value => String(value).replace(/\D/g, ''),
    ensureMongo: async () => true,
    apiMensajesConfirmacionCollection: () => col,
    apiMensajesConfirmacionId: to => to,
    arDatePartsForStats: () => ({ dayKey: '2026-09-17' }),
    apiMensajesConfirmacionAceptada: value => value?.estado === 'aceptado',
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

test('RVL no repite la solicitud si ya se pidió hoy aunque lleguen dos documentos', async () => {
  const state = setup('RVL', { estado: 'pendiente', solicitudDayKey: '2026-09-17', pedidoAt: new Date() });
  const result = await state.check('5491111111111', 'fc a 123');
  assert.equal(result.motivo, 'pendiente');
  assert.equal(state.sent, 0);
});

test('RVL vuelve a pedir al día siguiente aunque el cliente aceptó ayer', async () => {
  const state = setup('RVL', { estado: 'aceptado', solicitudDayKey: '2026-09-16', pedidoAt: new Date(Date.now() - 86400000), aceptadoAt: new Date(Date.now() - 80000000) });
  const result = await state.check('5491111111111', 'fc a 456');
  assert.equal(result.motivo, 'solicitud_enviada');
  assert.equal(state.sent, 1);
  assert.equal(state.updates[0].$set.solicitudDayKey, '2026-09-17');
});

test('RVL conserva la autorización de hoy y no pide de nuevo', async () => {
  const state = setup('RVL', { estado: 'aceptado', solicitudDayKey: '2026-09-17', pedidoAt: new Date(), aceptadoAt: new Date() });
  assert.equal((await state.check('5491111111111', 'fc a 789')).autorizado, true);
  assert.equal(state.sent, 0);
});

test('RVL vuelve a pedir después de una cancelación temporal de ayer', async () => {
  const state = setup('RVL', { estado: 'cancelado', solicitudDayKey: '2026-09-16', pedidoAt: new Date(Date.now() - 86400000), canceladoAt: new Date(Date.now() - 3600000) });
  assert.equal((await state.check('5491111111111', 'fc a 999')).motivo, 'solicitud_enviada');
  assert.equal(state.sent, 1);
});

test('RVL vuelve a pedir si ayer quedó pendiente sin respuesta', async () => {
  const state = setup('RVL', { estado: 'pendiente', solicitudDayKey: '2026-09-16', pedidoAt: new Date(Date.now() - 86400000) });
  assert.equal((await state.check('5491111111111', 'fc a 1000')).motivo, 'solicitud_enviada');
  assert.equal(state.sent, 1);
});

test('RVL respeta una exclusión permanente aunque cambie el día', async () => {
  const state = setup('RVL', { estado: 'cancelado', exclusionPermanente: true, exclusionMotivo: 'cancelar_cliente', solicitudDayKey: '2026-09-16', pedidoAt: new Date(Date.now() - 86400000) });
  assert.equal((await state.check('5491111111111', 'fc a 1001')).cancelarMensaje, true);
  assert.equal(state.sent, 0);
});

test('otros dominios conservan el permiso aceptado según su validez anterior', async () => {
  const state = setup('NEA', { estado: 'aceptado', solicitudDayKey: '2026-09-16', pedidoAt: new Date(Date.now() - 86400000), aceptadoAt: new Date() });
  assert.equal((await state.check('5491111111111', 'fc a 456')).autorizado, true);
  assert.equal(state.sent, 0);
});
