'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { isBrowserTransportError, canReplaySend, createBrowserRecovery } = require('../browser_transport_recovery');

test('recognizes NEA browser failures without treating auth or API errors as browser failures', () => {
  for (const message of ['Runtime.callFunctionOn timed out. Increase protocolTimeout', 'Attempted to use detached Frame 123', 'Protocol error (Runtime.callFunctionOn): Target closed', 'Execution context was destroyed']) {
    assert.equal(isBrowserTransportError(Error(message)), true);
  }
  for (const message of ['baileys_logged_out:401', 'NO EXISTE Usuario para Key', 'DAILY_MESSAGE_LIMIT', 'ETIMEDOUT']) assert.equal(isBrowserTransportError(Error(message)), false);
});

test('does not replay a dispatched send even when the remote acknowledgement is missing', () => {
  assert.equal(canReplaySend(true), false);
  assert.equal(canReplaySend(false), true);
});

test('coalesces concurrent failures and enforces cooldown across recovery failures', async () => {
  let time = 0, release, calls = 0;
  const recovery = createBrowserRecovery({ now: () => time, cooldownMs: 120000 });
  const error = Error('Attempted to use detached Frame');
  const first = recovery(error, async () => { calls++; await new Promise(r => { release = r; }); return true; });
  assert.equal(await recovery(error, async () => { calls++; }), false);
  release();
  assert.equal(await first, true);
  assert.equal(await recovery(error, async () => { calls++; }), false);
  time = 120000;
  await assert.rejects(recovery(error, async () => { calls++; throw Error('restart failed'); }), /restart failed/);
  assert.equal(await recovery(error, async () => { calls++; }), false);
  assert.equal(calls, 2);
});
