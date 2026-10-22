'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const source = require('node:fs').readFileSync(require.resolve('../app_asisto_ws'), 'utf8');
const fn = source.slice(source.indexOf('async function estadoCircuitBreakerApiMensajes('), source.indexOf('function esRespuestaNoValidaConfirmacionApiMensajes('));
function context(config) {
  const docs = Array.from({ length: 20 }, () => ({ estado: 'pendiente' }));
  const ctx = {
    tenantConfig: config,
    ensureMongo: async () => true,
    apiMensajesConfirmacionCollection: () => ({ find: () => ({ limit: () => ({ toArray: async () => docs }) }) }),
    arDatePartsForStats: () => ({ dayKey: '2026-10-22' }),
    apiMensajesConfirmacionTenantId: () => 'RVL', apiMensajesConfirmacionNumeroFrom: () => '123',
    api_mensajes_circuit_antiguedad_ms: 7200000, api_mensajes_circuit_min_muestra: 10,
    api_mensajes_circuit_sin_respuesta_ratio: 0.8
  };
  vm.createContext(ctx); vm.runInContext(fn, ctx); return ctx;
}
test('explicit disabled flag bypasses silence circuit, even with all requests unanswered', async () => {
  const ctx = context({ api_mensajes_circuit_habilitado: false });
  const result = await ctx.estadoCircuitBreakerApiMensajes();
  assert.equal(result.abierto, false);
  assert.equal(result.deshabilitado, true);
});
test('other domains retain their silence circuit by default', async () => {
  for (const cfg of [{}, { api_mensajes_circuit_habilitado: true }]) {
    const result = await context(cfg).estadoCircuitBreakerApiMensajes();
    assert.equal(result.abierto, true);
    assert.equal(result.muestra, 20);
  }
});
