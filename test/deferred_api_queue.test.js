'use strict';
const assert = require('assert/strict');
const { takeCustody, recover, RETRY_MS } = require('../deferred_api_queue');
const now = new Date('2026-10-07T19:00:00Z');
const item = { id_msj_dest: 1, id_msj_renglon: 2, content: 'PDF-data', msj: 'caption' };
async function custodyCase(override = {}, matches = 1, ackResult = true) {
  const calls = [];
  const doc = { _id: 'RVL:1:2', pendientes: { '1_2': { ...item, ...override } } };
  const collection = {
    findOne: async () => doc,
    updateOne: async (filter, update) => { calls.push({ filter, update }); return { matchedCount: matches }; }
  };
  const ok = await takeCustody({ collection, id: doc._id, key: '1_2', reason: 'lid_unresolved', now,
    acknowledge: async stored => {
      assert.equal(stored.content, 'PDF-data');
      assert.equal(calls.length, 1, 'durable schedule MUST precede API acknowledgement');
      assert.equal(calls[0].update.$set['deferredApi.active'], true);
      calls.push('ack'); return ackResult;
    }
  });
  return { ok, calls };
}
(async () => {
  const good = await custodyCase();
  assert.equal(good.ok, true);
  assert.equal(good.calls[0].update.$set['deferredApi.nextAt'].getTime(), now.getTime() + RETRY_MS);
  assert.ok(good.calls[2].update.$set['pendientes.1_2.apiEntregadoAt']);
  assert.equal(JSON.stringify(good.calls).includes('envioCompletadoAt":'), true); // filter only
  assert.equal(Object.keys(good.calls[0].update.$set).some(k => /envioCompletado|envioClaimed|estado/.test(k)), false);
  for (const override of [{ envioClaimedAt: now }, { envioCompletadoAt: now }, { id_msj_dest: null }]) {
    const r = await custodyCase(override); assert.equal(r.ok, false); assert.equal(r.calls.length, 0);
  }
  const lost = await custodyCase({}, 0); assert.equal(lost.ok, false); assert.equal(lost.calls.length, 1);
  const rejected = await custodyCase({}, 1, false); assert.equal(rejected.ok, false); assert.equal(rejected.calls.length, 2);
  // One failed recipient cannot starve the rest; schedule advances BEFORE processing.
  const docs = ['bad', 'good'].map(_id => ({ _id, deferredApi: { nextAt: now } }));
  const scheduled = [], processed = [], completed = [], errors = [];
  const collection = {
    find: q => { assert.equal(q['deferredApi.active'], true); return {
      sort: order => { assert.equal(order['deferredApi.nextAt'], 1); return {
        limit: n => { assert.equal(n, 3); return { toArray: async () => docs }; }
      }; }
    }; },
    updateOne: async (filter, update) => {
      (update.$set['deferredApi.active'] === false ? completed : scheduled).push(filter._id);
      return { matchedCount: 1 };
    }
  };
  await recover({ collection, baseQuery: { tenantId: 'RVL' }, now, allowed: async () => true,
    process: async d => { assert.ok(scheduled.includes(d._id)); processed.push(d._id); if(d._id==='bad')throw Error('lookup');return true; },
    onError: async d => errors.push(d._id) });
  assert.deepEqual(processed, ['bad', 'good']); assert.deepEqual(completed, ['good']); assert.deepEqual(errors, ['bad']);
  processed.length = 0;
  await recover({ collection, baseQuery: {}, now, allowed: async () => false, process: async d => processed.push(d), onError: async()=>{} });
  assert.equal(processed.length, 0);
  console.log('deferred_api_queue: OK');
})().catch(e => { console.error(e); process.exitCode = 1; });
