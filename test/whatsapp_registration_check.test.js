const assert = require('assert');
const { checkRegistration } = require('../whatsapp_registration_check');
(async () => {
  const opts = { delay: async () => {} };
  let calls = 0;
  assert.equal((await checkRegistration({ isRegisteredUser: async () => { calls++; if (calls < 3) throw Error('offline'); return true; } }, '123@c.us', opts)).registered, true);
  assert.equal(calls, 3);
  calls = 0;
  assert.equal((await checkRegistration({ isRegisteredUser: async () => ++calls > 1 }, '123@c.us', opts)).registered, true);
  assert.equal(calls, 2);
  for (const value of [false, undefined, null]) {
    const result = await checkRegistration({ isRegisteredUser: async () => value }, '123@c.us', opts);
    assert.equal(result.registered, false);
    assert.equal(result.retryable, true);
  }
  assert.equal((await checkRegistration({ isRegisteredUser: async () => { throw Error('network'); } }, '123@c.us', opts)).reason, 'lookup_error');
  const fs = require('fs');
  const source = fs.readFileSync(require('path').join(__dirname, '../app_asisto_ws.js'), 'utf8');
  const block = source.split('if (!registration.registered) {')[1].split('continue;')[0];
  assert(!/actualizarEstadoUnidad|eliminarPendiente|registrarExclusion/.test(block));
  console.log('Registration checks: ok');
})().catch(e => { console.error(e); process.exit(1); });
