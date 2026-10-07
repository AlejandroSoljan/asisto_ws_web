'use strict';
async function applyFastForward(run, repoPath, targetHead) {
  if (!/^[a-f0-9]{40}$/i.test(targetHead)) throw new Error('invalid_update_commit');
  // Git refuses divergence and overlapping local edits. Unrelated edits and
  // untracked customer configuration/backups remain untouched.
  return run('git', ['merge', '--ff-only', targetHead], { cwd: repoPath, timeout: 120000 });
}
function createMultiUpdateWatcher(initialTarget) {
  let seen = String(initialTarget || '').trim();
  return async function check({ enabled, primary, target, restart }) {
    const desired = String(target || '').trim();
    if (!enabled || !primary || !desired || desired === seen) return false;
    const previous = seen;
    seen = desired;
    try { await restart(desired); }
    catch (e) { seen = previous; throw e; }
    return true;
  };
}
module.exports = { applyFastForward, createMultiUpdateWatcher };
