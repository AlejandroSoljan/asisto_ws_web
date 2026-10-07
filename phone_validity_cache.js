'use strict';
const DAY = 86400000;
function ttl(config, valid) {
  const n = Number(config?.[valid ? 'phone_validation_valid_days' : 'phone_validation_invalid_days']);
  return (Number.isFinite(n) && n > 0 ? Math.min(n, 365) : valid ? 30 : 1) * DAY;
}
async function save(collection, identity, state, config, now, sent = false) {
  const validation = { state, checkedAt: new Date(now), expiresAt: new Date(now + ttl(config, state === 'valid')), source: sent ? 'successful_send' : 'whatsapp_lookup' };
  const set = { ...identity, phoneValidation: validation };
  delete set._id;
  if (sent) set.phoneLastSuccessfulSendAt = new Date(now);
  await collection.updateOne({ _id: identity._id }, { $set: set, $setOnInsert: { createdAt: new Date(now) } }, { upsert: true });
  return validation;
}
async function validate({ collection, identity, client, jid, config, now = Date.now(), delay = ms => new Promise(r => setTimeout(r, ms)) }) {
  if (!collection) return { state: 'unknown', reason: 'cache_unavailable' };
  try {
    const doc = await collection.findOne({ _id: identity._id });
    const cached = doc?.phoneValidation;
    // Negative library lookups can be false negatives, even when repeated.
    // Never trust the negative cache produced by previous versions.
    if (cached?.state === 'valid' && new Date(cached.expiresAt).getTime() > now) return { ...cached, cached: true };
    let negatives = 0;
    for (let i = 0; i < 3; i++) {
      try {
        const result = await client.isRegisteredUser(jid);
        if (result === true) return await save(collection, identity, 'valid', config, now);
        if (result === false) negatives++;
      } catch { /* An exception never proves a phone is invalid. */ }
      if (i < 2) await delay(1500);
    }
    return { state: 'unknown', reason: negatives === 3 ? 'lookup_negative_unconfirmed' : 'lookup_inconclusive' };
  } catch {
    return { state: 'unknown', reason: 'cache_error' };
  }
}
function canAttemptDelivery(result) {
  return result?.state === 'valid' || (result?.state === 'unknown' &&
    ['lookup_negative_unconfirmed', 'lookup_inconclusive'].includes(result.reason));
}
module.exports = { validate, save, ttl, canAttemptDelivery };
