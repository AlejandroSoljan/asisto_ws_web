'use strict';

// One cleanup per authentication attempt. Only a successful connection rearms it.
function createAuthFailureRecovery() {
  let attempted = false;
  let running = false;
  return {
    reset() { attempted = false; },
    async run(recover) {
      if (attempted || running) return { skipped: true };
      attempted = true;
      running = true;
      try { return { skipped: false, ok: await recover() }; }
      finally { running = false; }
    }
  };
}

module.exports = { createAuthFailureRecovery };
