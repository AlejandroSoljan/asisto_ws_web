'use strict';

// Persistent reservations: an uncertain transport failure still consumes a slot.
// A restart must never reset the quota or retry an ambiguous send for free.
async function reserve(collection, { tenantId, numeroFrom, dayKey, limit, sent }) {
  if (!(limit > 0)) return true;
  if (!collection || !Number.isFinite(sent)) throw new Error('daily_quota_unavailable');
  const query = { _id: `daily-quota:${tenantId}:${numeroFrom}:${dayKey}` };
  try {
    await collection.updateOne(query, { $setOnInsert: {
      tenantId, numeroFrom, channelType: 'api_daily_quota', dayKey, used: 0
    } }, { upsert: true });
  } catch (e) {
    if (Number(e.code) !== 11000) throw e;
  }
  await collection.updateOne(query, { $max: { used: sent } });
  const result = await collection.updateOne({ ...query, used: { $lt: limit } }, {
    $inc: { used: 1 }, $set: { updatedAt: new Date() }
  });
  return Number(result?.matchedCount) === 1;
}

module.exports = { reserve };
