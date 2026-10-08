const { test } = require('node:test');
const assert = require('node:assert/strict');
const { reserve } = require('../daily_message_quota');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require.resolve('../app_asisto_ws'), 'utf8');

function collection() {
  const docs = new Map();
  return { docs, async updateOne(q, u) {
    if (u.$setOnInsert && !docs.has(q._id)) docs.set(q._id, { ...u.$setOnInsert });
    const d = docs.get(q._id);
    if (!d || (q.used && d.used >= q.used.$lt)) return { matchedCount: 0 };
    if (u.$max) d.used = Math.max(d.used, u.$max.used);
    if (u.$inc) d.used += u.$inc.used;
    return { matchedCount: 1 };
  } };
}
const args = { tenantId: 'RVL', numeroFrom: '5493462239802', dayKey: '2026-10-08', limit: 120, sent: 0 };
test('120 atomic reservations, concurrent request 121 denied', async () => {
  const c = collection();
  const results = await Promise.all(Array.from({ length: 150 }, () => reserve(c, args)));
  assert.equal(results.filter(Boolean).length, 120);
  assert.equal(await reserve(c, args), false); // restart: persisted quota
});
test('seeds earlier messages, cannot reset with stale stats, next day independent', async () => {
  const c = collection();
  assert.equal(await reserve(c, { ...args, sent: 119 }), true);
  assert.equal(await reserve(c, args), false);
  assert.equal(await reserve(c, { ...args, dayKey: '2026-10-09' }), true);
  assert.equal(await reserve(c, { ...args, tenantId: 'NEA' }), true);
});
test('unlimited does not require storage, limited fails closed', async () => {
  assert.equal(await reserve(null, { ...args, limit: 0 }), true);
  await assert.rejects(reserve(null, args), /unavailable/);
  await assert.rejects(reserve(collection(), { ...args, sent: undefined }), /unavailable/);
});
test('counts every attachment, deduplicates IDs, accepted still blocked by 120', async () => {
  const context = {
    tenantId: 'RVL', numero: '123', tenantConfig: { api_mensajes_limite_unidad: 'clientes', api_mensajes_limite_mensajes_diario: 120 },
    api_mensajes_limite_diario: 50, ensureMongo: async () => true, getApiMensajesNroTelFrom: () => '123',
    arDatePartsForStats: () => ({ dayKey: '2026-10-08' }), onlyDigits: s => s.replace(/\D/g, ''),
    getDataCollection: () => ({ find: () => ({ toArray: async () => [{ _id: 'w', contact: '456', messages: Array.from({ length: 120 }, (_, i) => ({ at: '2026-10-08T12:00:00Z', waMessageId: 'id' + i })) }] }), findOne: async () => null })
  };
  vm.createContext(context);
  vm.runInContext(source.slice(source.indexOf('async function estadoLimiteDiarioApiMensajes('), source.indexOf('function logLimiteDiarioApiMensajes(')), context);
  assert.equal((await context.estadoLimiteDiarioApiMensajes('456', true)).permitido, false);
  context.tenantConfig.api_mensajes_limite_mensajes_diario = 0;
  assert.equal((await context.estadoLimiteDiarioApiMensajes('456', true)).permitido, true);
});
test('quota denial releases unsent claim and never calls WhatsApp', async () => {
  let released = false;
  const context = {
    tenantConfig: { api_mensajes_limite_mensajes_diario: 120 }, onlyDigits: s => s,
    estadoLimiteDiarioApiMensajes: async () => ({ permitido: false }),
    keyPendienteConfirmacionApiMensajes: () => '1_2', apiMensajesConfirmacionId: () => 'RVL:123:456',
    apiMensajesConfirmacionCollection: () => ({ updateOne: async (q, u) => { released = !!u.$unset['pendientes.1_2.envioClaimedAt'] || Object.hasOwn(u.$unset, 'pendientes.1_2.envioClaimedAt'); } })
  };
  vm.createContext(context);
  vm.runInContext(source.slice(source.indexOf('let apiMensajesEnvioTail'), source.indexOf('function prepararUnidadesApiMensajes(')), context);
  await assert.rejects(context.enviarApiMensajesConPausa('456', () => assert.fail('must not send'), { idDest: 1, idRenglon: 2 }), { code: 'DAILY_MESSAGE_LIMIT' });
  assert.equal(released, true);
});
