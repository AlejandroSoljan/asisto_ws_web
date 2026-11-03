"use strict";
function install(client) {
  if (!client || typeof client.inject !== 'function' || client.__authReadinessInstalled) return;
  const inject=client.inject;
  client.inject=async function(...args) {
    if (!this.pupPage) throw Error('wweb_auth_page_unavailable');
    // The upstream injector dereferences this module immediately. Wait for the
    // real module, not just Debug.VERSION, without changing session storage.
    await this.pupPage.waitForFunction(() => {
      try { return !!window.require?.('WAWebSocketModel')?.Socket; } catch { return false; }
    }, {timeout:60000,polling:250});
    return inject.apply(this,args);
  };
  client.__authReadinessInstalled=true;
}
module.exports={install};
