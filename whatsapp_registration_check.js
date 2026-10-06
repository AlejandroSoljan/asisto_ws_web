'use strict';

// A lookup is not proof that a customer opted out or that a pending message
// should be deleted. Retry boundedly; defer inconclusive/negative results.
async function checkRegistration(client, jid, { attempts = 3, delay = ms => new Promise(r => setTimeout(r, ms)) } = {}) {
  let reason = 'lookup_negative';
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      if (await client.isRegisteredUser(jid) === true) return { registered: true };
      reason = 'lookup_negative';
    } catch {
      reason = 'lookup_error';
    }
    if (attempt + 1 < attempts) await delay(1500);
  }
  return { registered: false, retryable: true, reason };
}

module.exports = { checkRegistration };
