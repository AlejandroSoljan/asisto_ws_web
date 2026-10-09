const { test } = require('node:test');
const assert = require('node:assert/strict');
const { effectiveDocument } = require('../technical_exclusion');
test('historical timeout becomes pending, never accepted; explicit opt-out stays', () => {
  const doc = { estado: 'cancelado', motivoCancelacion: 'sin_respuesta_timeout' };
  assert.equal(effectiveDocument(doc).estado, 'pendiente');
  assert.equal(effectiveDocument({ ...doc, exclusionPermanente: true }).estado, 'cancelado');
  assert.equal(effectiveDocument({ ...doc, respuestaCancelacion: 'NO' }).estado, 'cancelado');
});
test('timeout scheduler only queues a retry with race and opt-out guards', async () => {
  const source = require('node:fs').readFileSync(require.resolve('../app_asisto_ws'), 'utf8');
  const vm = require('node:vm');
  const writes = [];
  const doc = { _id: 'test', nroTel: '123', pedidoAt: new Date(0) };
  const ctx = { api_mensajes_confirmacion_habilitada: true, api_mensajes_confirmacion_reenviar_ms: 1000,
    ensureMongo: async () => true, apiMensajesConfirmacionTenantId: () => 'NEA', apiMensajesConfirmacionNumeroFrom: () => '456',
    apiMensajesConfirmacionCollection: () => ({ find: () => ({ limit: () => ({ toArray: async () => [doc] }) }), updateOne: async (q,u) => writes.push({q,u}) }),
    console: { log() {} }, EscribirLog() {},
    procesarPendientesDocConfirmacionApiMensajes: () => assert.fail('must not cancel/send') };
  vm.createContext(ctx);
  vm.runInContext(source.slice(source.indexOf('async function procesarTimeoutsPendientesConfirmacionApiMensajes('), source.indexOf('function normalizarRespuestaConfirmacionApiMensajes(')), ctx);
  await ctx.procesarTimeoutsPendientesConfirmacionApiMensajes();
  assert.equal(writes.length, 1);
  assert.equal(writes[0].u.$set['deferredApi.active'], true);
  assert.equal(writes[0].u.$set.estado, undefined);
  assert.equal(writes[0].q.estado, 'pendiente');
  assert.equal(writes[0].q.exclusionPermanente.$ne, true);
  assert.equal(writes[0].q['deferredApi.active'].$ne, true);
  assert.ok(source.includes('const debePedir = expiroVentana ||'));
  assert.ok(!source.includes("cancelarMensaje: true, doc: { ...(doc || {}), ...setCancelado }"));
});
test('legacy lookup exclusion is not a cancellation or consent', () => {
  const old = { estado: 'cancelado', exclusionPermanente: true, exclusionMotivo: 'numero_no_registrado', motivoCancelacion: 'numero_no_registrado', respuestaCancelacion: '' };
  const result = effectiveDocument(old);
  assert.equal(result.estado, 'pendiente');
  assert.equal(result.exclusionPermanente, undefined);
  assert.equal(old.exclusionPermanente, true);
});
test('preserves customer opt-out and unknown exclusion', () => {
  for (const reason of ['cancelar_cliente', 'baja', 'manual']) {
    const doc = { exclusionPermanente: true, exclusionMotivo: reason };
    assert.equal(effectiveDocument(doc), doc);
  }
  const doc = { exclusionMotivo: 'numero_no_registrado', motivoCancelacion: 'numero_no_registrado', respuestaCancelacion: 'CANCELAR' };
  assert.equal(effectiveDocument(doc), doc);
});
