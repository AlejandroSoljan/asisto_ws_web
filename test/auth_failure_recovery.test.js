'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { createAuthFailureRecovery } = require('../auth_failure_recovery');

test('duplicate events cannot clear authentication concurrently or repeatedly', async () => {
  const recovery = createAuthFailureRecovery();
  let release;
  let calls = 0;
  const first = recovery.run(async () => { calls++; await new Promise(r => { release = r; }); return true; });
  assert.equal((await recovery.run(() => { calls++; })).skipped, true);
  release();
  assert.deepEqual(await first, { skipped: false, ok: true });
  assert.equal((await recovery.run(() => { calls++; })).skipped, true);
  assert.equal(calls, 1);
  recovery.reset(); // ready: a newly connected session can subsequently expire.
  assert.equal((await recovery.run(async () => true)).ok, true);
});

test('failed cleanup waits for manual intervention instead of looping', async () => {
  const recovery = createAuthFailureRecovery();
  await assert.rejects(recovery.run(async () => { throw Error('file locked'); }), /file locked/);
  assert.equal((await recovery.run(async () => true)).skipped, true);
});

test('recovery state is isolated per worker', async () => {
  const first = createAuthFailureRecovery(), second = createAuthFailureRecovery();
  await first.run(async () => true);
  assert.equal((await second.run(async () => true)).ok, true);
});
