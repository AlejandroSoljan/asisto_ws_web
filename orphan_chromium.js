"use strict";
const fs = require('fs');
const path = require('path');
async function recover(profile, { platform = process.platform, io = fs, signal = process.kill.bind(process), currentPid = process.pid, pause = ms => new Promise(r=>setTimeout(r,ms)) } = {}) {
  if (platform !== 'linux' || !path.isAbsolute(profile)) return { recovered:false, reason:'unsupported' };
  const wanted = path.resolve(profile);
  function matching() {
    return io.readdirSync('/proc').filter(n=>/^\d+$/.test(n)).flatMap(n=>{
      try {
        const args=io.readFileSync('/proc/'+n+'/cmdline','utf8').split('\0');
        if (!/^(chrome|chromium|chromium-browser|google-chrome)(-|$)/.test(path.basename(args[0]||''))) return [];
        if (args.some(a=>a.startsWith('--type='))) return [];
        const i=args.indexOf('--user-data-dir');
        const raw=args.find(a=>a.startsWith('--user-data-dir='))?.slice(16) || (i>=0?args[i+1]:'');
        if (!raw || path.resolve(raw)!==wanted) return [];
        const stat=io.readFileSync('/proc/'+n+'/stat','utf8');
        const ppid=Number(stat.slice(stat.lastIndexOf(')')+2).split(' ')[1]);
        return [{pid:Number(n),ppid}];
      } catch { return []; }
    });
  }
  const browsers=matching();
  if (browsers.some(b=>b.ppid!==1 && b.ppid!==currentPid)) return {recovered:false,reason:'other_live_owner'};
  for (const b of browsers) { try {signal(b.pid,'SIGTERM')}catch(e){if(e.code!=='ESRCH')throw e} }
  if (browsers.length) await pause(1000);
  for (const b of matching()) { try {signal(b.pid,'SIGKILL')}catch(e){if(e.code!=='ESRCH')throw e} }
  if (browsers.length) await pause(300);
  if (matching().length) return {recovered:false,reason:'browser_still_running'};
  // Do not touch authentication or lock files: Chromium manages its profile lock.
  return {recovered:browsers.length>0,reason:browsers.length?'orphan_closed':'no_orphan',count:browsers.length};
}
module.exports={recover};
