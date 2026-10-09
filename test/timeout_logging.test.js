const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const source = require('node:fs').readFileSync(require.resolve('../app_asisto_ws'), 'utf8');
const fn = source.slice(source.indexOf('async function procesarTimeoutsPendientesConfirmacionApiMensajes('), source.indexOf('function normalizarRespuestaConfirmacionApiMensajes('));
async function run(docs, modifiedCount) {
  let filter, writes = 0, logs = 0;
  const ctx = { api_mensajes_confirmacion_habilitada: true, api_mensajes_confirmacion_reenviar_ms: 1000,
    ensureMongo: async () => true, apiMensajesConfirmacionTenantId: () => 'RVL', apiMensajesConfirmacionNumeroFrom: () => '123',
    apiMensajesConfirmacionCollection: () => ({ find(q) { filter=q; return { limit: () => ({ toArray: async () => docs }) }; }, updateOne: async () => { writes++; return { modifiedCount }; } }),
    console: { log() { logs++; } }, EscribirLog() {} };
  vm.createContext(ctx); vm.runInContext(fn, ctx);
  await ctx.procesarTimeoutsPendientesConfirmacionApiMensajes();
  return { filter, writes, logs };
}
const pending = { _id: 'x', pedidoAt: new Date(0), pendientes: { '1_2': { msj: 'test' } } };
test('empty, missing, already scheduled and opted-out records produce no writes/logs', async () => {
  const r = await run([{}, { pendientes: {} }, { pendientes: null }, { ...pending, deferredApi: { active: true } }, { ...pending, exclusionPermanente: true }], 1);
  assert.equal(r.writes, 0); assert.equal(r.logs, 0);
  assert.equal(r.filter['deferredApi.active'].$ne, true);
  assert.equal(r.filter.pendientes.$nin.length, 2);
});
test('concurrent no-op produces no misleading scheduled log', async () => {
  const r = await run([pending], 0); assert.equal(r.writes, 1); assert.equal(r.logs, 0);
});
test('successful scheduling produces one log', async () => {
  const r = await run([pending], 1); assert.equal(r.writes, 1); assert.equal(r.logs, 1);
});
