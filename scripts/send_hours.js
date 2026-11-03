"use strict";
const TZ = 'America/Argentina/Buenos_Aires';
function evaluate(hours, date = new Date(), enabled = true) {
  if (!enabled) return { allowed: true, reason: 'schedule_disabled' };
  if (!hours || !Object.keys(hours).some(d => Array.isArray(hours[d]) && hours[d].length))
    return { allowed: false, reason: 'schedule_unavailable' };
  const dayKey = new Intl.DateTimeFormat('en-US', { timeZone: TZ, weekday: 'long' }).format(date).toLowerCase();
  const hhmm = new Intl.DateTimeFormat('sv-SE', { timeZone: TZ, hour:'2-digit', minute:'2-digit', hourCycle:'h23' }).format(date);
  const mins = v => /^\d{2}:\d{2}$/.test(String(v)) && +v.slice(0,2)<24 && +v.slice(3)<60 ? +v.slice(0,2)*60 + +v.slice(3) : NaN;
  const now = mins(hhmm), ranges = (hours[dayKey] || []).filter(r => mins(r.from) < mins(r.to));
  // Closing minute is excluded: 19:00 means no dispatch at or after 19:00.
  const allowed = ranges.some(r => now >= mins(r.from) && now < mins(r.to));
  return { allowed, reason: allowed ? 'inside_range' : (ranges.length ? 'outside_range' : 'day_closed'), dayKey, hhmm, slots: ranges.map(r=>r.from+'-'+r.to).join(', ') };
}
async function waitUntilAllowed(status, sleep, onBlocked = () => {}) {
  for (;;) {
    const s = await status();
    if (s.allowed) return s;
    onBlocked(s);
    await sleep(30000);
  }
}
module.exports = { evaluate, waitUntilAllowed };
