const {test}=require('node:test');const assert=require('node:assert/strict');const {evaluate,waitUntilAllowed}=require('../send_hours');const fs=require('node:fs');const vm=require('node:vm');
const hours={tuesday:[{from:'08:00',to:'19:00'}]};
test('Argentina midnight, opening, closing, closed day and unavailable schedule',()=>{
 for(const [at,allowed] of [['2026-11-03T03:02:04Z',false],['2026-11-03T10:59:59Z',false],['2026-11-03T11:00:00Z',true],['2026-11-03T21:59:59Z',true],['2026-11-03T22:00:00Z',false],['2026-11-04T12:00:00Z',false]])assert.equal(evaluate(hours,new Date(at)).allowed,allowed,at);
 assert.equal(evaluate(null).allowed,false);assert.equal(evaluate(null,new Date(),false).allowed,true);
});
test('dispatch waits until allowed and checks every retry',async()=>{let checks=0,waits=0;await waitUntilAllowed(async()=>({allowed:++checks===3}),async()=>{waits++});assert.equal(waits,2);assert.equal(checks,3)});
const source=fs.readFileSync(require.resolve('../app_asisto_ws'),'utf8');
test('safeSend never calls transport while closed, including retry crossing closing time',async()=>{
 const fn=source.slice(source.indexOf('async function safeSend('),source.indexOf('async function updateLockStateSafe',source.indexOf('async function safeSend(')));
 let dispatch=0,checks=0;const ctx={isWwebMessagesBlockedSafe:async()=>false,isWwebJsEngine:()=>false,client:{getState:async()=> 'CONNECTED',sendMessage:async()=>{dispatch++;throw Error('baileys_not_connected')}},sleep:async()=>{},browserRecoveryScheduled:false,restartInFlight:false,clearAuthInFlight:false,waitForSendHours:async()=>{if(++checks===2)throw Error('test_closed')},automaticSendIds:new Map(),automaticSendsInFlight:new Set(),requestBrowserRecovery:()=>{},browserTransportRecovery:{canReplaySend:()=>true}};
 vm.createContext(ctx);vm.runInContext(fn,ctx);await assert.rejects(ctx.safeSend('x','text'),/test_closed/);assert.equal(dispatch,1);assert.equal(checks,2);
});
test('accepted pending sends check hours before claiming; queue checks after delay before quota',()=>{
 const pending=source.slice(source.indexOf('async function procesarPendientesDocConfirmacionApiMensajes'),source.indexOf('async function procesarPendientesConfirmacionApiMensajes'));
 assert.ok(pending.indexOf('getConsultaMensajesScheduleStatus()')<pending.indexOf('const claimAt'));
 const queue=source.slice(source.indexOf('async function enviarApiMensajesConPausa'),source.indexOf('function prepararUnidadesApiMensajes'));
 assert.ok(queue.indexOf('await sleep(delay)')<queue.indexOf('await waitForSendHours()'));assert.ok(queue.indexOf('await waitForSendHours()')<queue.indexOf("require('./daily_message_quota').reserve"));
 assert.equal((source.match(/client\.sendMessage\(/g)||[]).length,1);
});
