'use strict';

function isBrowserTransportError(error) {
  const message = String(error?.message || error || '');
  return /detached\s+frame|frame.*detached|execution context was destroyed|runtime\.callfunctionon.*timed out|protocol error|target closed|session closed|connection closed.*protocol|browser.*disconnected|wwebjs_media_send_unconfirmed_id/i.test(message);
}

function canReplaySend(dispatchStarted) {
  // A browser timeout does not tell us whether WhatsApp accepted the message.
  return dispatchStarted !== true;
}

function createBrowserRecovery({ now = Date.now, cooldownMs = 120000 } = {}) {
  let running = false;
  let lastAttempt = -Infinity;
  return async function recover(error, restart) {
    if (!isBrowserTransportError(error) || running || now() - lastAttempt < cooldownMs) return false;
    running = true;
    lastAttempt = now();
    try { return await restart(); }
    finally { running = false; }
  };
}

module.exports = { isBrowserTransportError, canReplaySend, createBrowserRecovery };
