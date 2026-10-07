'use strict';
const RETRY_MS = 5 * 60 * 1000;

// API E is the existing custody acknowledgement, not WhatsApp delivery.
// Never acknowledge until the complete item and its retry schedule are durable.
async function takeCustody({ collection, id, key, reason, acknowledge, now = new Date() }) {
  if (!/^\d+_\d+$/.test(key)) throw new Error('invalid_pending_key');
  const doc = await collection.findOne({ _id: id });
  const item = doc?.pendientes?.[key];
  if (!item || item.envioClaimedAt || item.envioCompletadoAt ||
      !item.id_msj_dest || !item.id_msj_renglon ||
      (item.content == null && !item.msj)) return false;
  const filter = { _id: id, [`pendientes.${key}`]: { $exists: true },
    [`pendientes.${key}.envioClaimedAt`]: { $exists: false },
    [`pendientes.${key}.envioCompletadoAt`]: { $exists: false } };
  const result = await collection.updateOne(filter, { $set: {
    'deferredApi.active': true, 'deferredApi.reason': reason,
    'deferredApi.nextAt': new Date(now.getTime() + RETRY_MS),
    'deferredApi.updatedAt': now,
    [`pendientes.${key}.custodyAt`]: now
  } });
  if (result?.matchedCount !== 1) return false;
  if (!await acknowledge(item)) return false;
  await collection.updateOne(filter, { $set: { [`pendientes.${key}.apiEntregadoAt`]: now } });
  return true;
}

async function recover({ collection, baseQuery, allowed, process, onError, now = new Date(), limit = 3 }) {
  const docs = await collection.find({ ...baseQuery, 'deferredApi.active': true,
    'deferredApi.nextAt': { $lte: now } }).sort({ 'deferredApi.nextAt': 1, _id: 1 }).limit(limit).toArray();
  for (const doc of docs) {
    if (!await allowed()) break;
    const result = await collection.updateOne({ _id: doc._id,
      'deferredApi.active': true, 'deferredApi.nextAt': doc.deferredApi.nextAt },
    { $set: { 'deferredApi.nextAt': new Date(now.getTime() + RETRY_MS), 'deferredApi.updatedAt': now } });
    if (result?.matchedCount !== 1) continue;
    try {
      if (await process(doc)) await collection.updateOne({ _id: doc._id },
        { $set: { 'deferredApi.active': false, 'deferredApi.updatedAt': new Date() } });
    } catch (error) { await onError(doc, error); }
  }
}

module.exports = { takeCustody, recover, RETRY_MS };
