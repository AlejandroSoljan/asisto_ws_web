'use strict';
const assert = require('assert/strict');
const { installInPage, prepareRecipient } = require('../wweb_chat_resolution');
const phone = '5493463521152@c.us';
const lid = '123456789012345@lid';
async function run(errorText, mapping) {
  const calls = [];
  global.window = { WWebJS: {
    getChat: async (id, options) => {
      calls.push({ id, options });
      if (id === phone) throw new Error(errorText);
      return { id };
    },
    enforceLidAndPnRetrieval: async () => mapping
  } };
  installInPage();
  const installed = window.WWebJS.getChat;
  installInPage();
  assert.equal(window.WWebJS.getChat, installed);
  try { return { result: await installed(phone, { getAsModel: false }), calls }; }
  catch (error) { return { error: error.message, calls }; }
}
(async () => {
  const good = { phone: { _serialized: phone }, lid: { _serialized: lid } };
  const recovered = await run('No LID for user', good);
  assert.equal(recovered.result.id, lid);
  assert.deepEqual(recovered.calls.map(c => c.id), [phone, lid]);
  assert.deepEqual(recovered.calls[1].options, { getAsModel: false });
  for (const map of [{}, { ...good, phone: { _serialized: '999@c.us' } }, { ...good, lid: { _serialized: phone } }]) {
    const r = await run('No LID for user', map);
    assert.equal(r.error, 'No LID for user');
    assert.equal(r.calls.length, 1);
  }
  const unrelated = await run('Protocol error', good);
  assert.equal(unrelated.error, 'Protocol error');
  assert.equal(unrelated.calls.length, 1);
  let count = 0;
  window.WWebJS = { getChat: async () => { count++; return { ok: true }; } };
  installInPage();
  assert.deepEqual(await window.WWebJS.getChat(phone), { ok: true });
  assert.equal(count, 1);
  const fakeClient = { pupPage: { evaluate: async (fn, arg) => fn(arg) } };
  window.WWebJS = { getChat: async id => {
    if (id === phone) throw new Error('No LID for user');
    return { id };
  } };
  const batch = [];
  for (const jid of [phone, '5493462555047@c.us']) {
    if (!(await prepareRecipient(fakeClient, jid)).ready) continue;
    batch.push(jid);
  }
  assert.deepEqual(batch, ['5493462555047@c.us']);
  window.WWebJS = { getChat: async () => { throw new Error('Protocol error'); } };
  await assert.rejects(prepareRecipient(fakeClient, phone), /Protocol error/);
  window.WWebJS = { getChat: async () => null };
  assert.equal((await prepareRecipient(fakeClient, phone)).ready, false);
  assert.equal((await prepareRecipient({}, phone)).ready, true);
  delete global.window;
  console.log('wweb_chat_resolution: OK');
})().catch(e => { console.error(e); process.exitCode = 1; });
