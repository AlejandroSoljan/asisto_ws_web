const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const source = require('node:fs').readFileSync(require.resolve('../app_asisto_ws'), 'utf8');
const fn = source.slice(source.indexOf('function solicitudPrioridadBloqueada('), source.indexOf('async function registrarRespuestaConfirmacionApiMensajes('));
function context(doc, enabled = true) {
  const ctx = { tenantConfig: { api_mensajes_solicitudes_prioridades_bloqueadas: [2] },
    api_mensajes_confirmacion_habilitada: enabled, api_mensajes_confirmacion_reenviar_ms: 1000,
    onlyDigits: x => x, requiereConfirmacionPrioridadApiMensajes: () => true, ensureMongo: async () => true,
    apiMensajesConfirmacionId: x => x, apiMensajesConfirmacionCollection: () => ({ findOne: async () => doc }),
    require: () => ({ effectiveDocument: d => d }), apiMensajesConfirmacionSolicitudVigente: () => true,
    apiMensajesConfirmacionAceptada: d => d?.estado === 'aceptado',
    detectarNoValidaConfirmacionApiMensajesEnChat: async () => false,
    detectarOkConfirmacionApiMensajesEnChat: async () => false,
    estadoLimiteDiarioApiMensajes: async () => ({ permitido: false }), logLimiteDiarioApiMensajes() {}
  };
  vm.createContext(ctx); vm.runInContext(fn, ctx); return ctx;
}
test('priority 2 cannot request consent, whether new or overdue', async () => {
  for (const doc of [null, { estado: 'pendiente', pedidoAt: new Date(1) }]) {
    const r = await context(doc).estadoConfirmacionApiMensajes('123', '', 2);
    assert.equal(r.motivo, 'solicitudes_prioridad_suspendidas');
    assert.equal(r.autorizado, false); assert.equal(r.cancelarMensaje, undefined);
  }
});
test('accepted remains authorized, even with request suspension', async () => {
  assert.equal((await context({ estado: 'aceptado' }).estadoConfirmacionApiMensajes('123', '', 2)).autorizado, true);
});
test('disabled confirmations cannot bypass suspension; priority 1 unchanged', async () => {
  assert.equal((await context(null, false).estadoConfirmacionApiMensajes('123', '', 2)).autorizado, false);
  assert.equal((await context(null).estadoConfirmacionApiMensajes('123', '', 1)).motivo, 'limite_diario');
});
