'use strict';
// Transport flow-control is deliberately embedded in each extension.
// Packetfactory-TS owns client->backend (C2S); Dimensions-MCL owns backend->client (S2C).
// These directions pause DIFFERENT source sockets; there is no third controller.
// This module never alters, queues, merges, reorders or drops Terraria packets.
const COORD=Symbol.for('dimensions.packetts.mobileclp.transport-owner.v1');
const DEFAULTS={enabled:true,tcp:{noDelay:true,keepAlive:true,keepAliveInitialDelayMs:30000},backpressure:{enabled:true,highWaterBytes:524288,lowWaterBytes:131072,maxPauseMs:2500,retryCooldownMs:3000},logging:{warnOnCongestion:true,warningCooldownMs:30000,logSummary:false,summaryIntervalSeconds:120}};
function clamp(v,a,b,def){const n=Number(v);return Number.isFinite(n)?Math.min(b,Math.max(a,Math.trunc(n))):def;}
function parseConfig(input){
 const u=input||{};
 const c={...DEFAULTS,...u,tcp:{...DEFAULTS.tcp,...u.tcp},backpressure:{...DEFAULTS.backpressure,...u.backpressure},logging:{...DEFAULTS.logging,...u.logging}};
 c.tcp.keepAliveInitialDelayMs=clamp(c.tcp.keepAliveInitialDelayMs,1000,300000,30000);
 c.backpressure.highWaterBytes=clamp(c.backpressure.highWaterBytes,65536,16777216,524288);
 c.backpressure.lowWaterBytes=clamp(c.backpressure.lowWaterBytes,16384,c.backpressure.highWaterBytes-1,131072);
 c.backpressure.maxPauseMs=clamp(c.backpressure.maxPauseMs,500,30000,2500);
 c.backpressure.retryCooldownMs=clamp(c.backpressure.retryCooldownMs,0,60000,3000);
 c.logging.warningCooldownMs=clamp(c.logging.warningCooldownMs,1000,3600000,30000);
 c.logging.summaryIntervalSeconds=clamp(c.logging.summaryIntervalSeconds,10,3600,120);
 return c;
}
class TransportFlow {
 constructor(logging,config,direction,prefix){
  if(direction!=='C2S'&&direction!=='S2C')throw new Error('Invalid transport direction');
  this.logging=logging||console;this.config=parseConfig(config);this.direction=direction;this.prefix=prefix;
  this.tuned=new WeakSet();this.holds=new Map();this.cooldowns=new WeakMap();this.lastWarning=0;
  this.stats={tuned:0,pauses:0,releases:0,timeouts:0,peakQueueBytes:0,bytes:0};
  if(this.config.enabled){
   this.maintenanceTimer=setInterval(()=>this.maintain(),250);this.maintenanceTimer.unref?.();
   if(this.config.logging.logSummary){this.summaryTimer=setInterval(()=>this.report(),this.config.logging.summaryIntervalSeconds*1000);this.summaryTimer.unref?.();}
  }
 }
 log(s){try{this.logging.info?.(`[${this.prefix}] ${s}`);}catch{}}
 warn(s){if(!this.config.logging.warnOnCongestion)return;const now=Date.now();if(now-this.lastWarning<this.config.logging.warningCooldownMs)return;this.lastWarning=now;this.log(s);}
 tune(socket){
  if(!this.config.enabled||!socket||socket.destroyed||this.tuned.has(socket)||typeof socket.setNoDelay!=='function')return;
  this.tuned.add(socket);
  try{if(this.config.tcp.noDelay)socket.setNoDelay(true);if(this.config.tcp.keepAlive)socket.setKeepAlive(true,this.config.tcp.keepAliveInitialDelayMs);this.stats.tuned++;}
  catch(e){this.warn('socket-tune-error '+(e?.message||'unknown'));}
 }
 socketConnect(socket){this.tune(socket);}
 clientConnect(client){this.tune(client?.socket);this.tune(client?.server?.socket);}
 toServer(server,buf){
  if(!this.config.enabled||this.direction!=='C2S'||!server?.client)return;
  this.stats.bytes+=Buffer.isBuffer(buf)?buf.length:0;
  this.tune(server.socket);
  if(server.client.server===server)this.check(server.client.socket,server.socket,server.client);
 }
 toClient(client,buf){
  if(!this.config.enabled||this.direction!=='S2C'||!client)return;
  this.stats.bytes+=Buffer.isBuffer(buf)?buf.length:0;
  this.tune(client.socket);this.tune(client.server?.socket);
  if(client.connected&&client.server?.socket)this.check(client.server.socket,client.socket,client);
 }
 check(source,sink,client){
  if(!this.config.backpressure.enabled||!source||!sink||source===sink||source.destroyed||sink.destroyed||!source.readable||!sink.writable)return;
  const queued=Math.max(0,Number(sink.writableLength)||0);this.stats.peakQueueBytes=Math.max(queued,this.stats.peakQueueBytes);
  const existing=this.holds.get(source);
  if(existing){
   if(existing.sink!==sink)this.release(existing,'switched');
   else{if(queued<=this.config.backpressure.lowWaterBytes)this.release(existing,'low-water');return;}
  }
  if(queued<this.config.backpressure.highWaterBytes||Date.now()<(this.cooldowns.get(source)||0))return;
  // Only ONE direction may pause a client's incoming socket at once.
  // Prevent C2S and S2C from pausing each other during simultaneous congestion.
  if(client[COORD]&&client[COORD].source!==source)return;
  // Don't unpause a socket that the original Dimensions or another extension paused.
  if(typeof source.pause!=='function'||typeof source.resume!=='function'||source.isPaused?.())return;
  const hold={source,sink,client,started:Date.now()};
  hold.drain=()=>{if(sink.destroyed||(Number(sink.writableLength)||0)<=this.config.backpressure.lowWaterBytes)this.release(hold,'drain');};
  hold.close=()=>this.release(hold,'close');
  this.holds.set(source,hold);client[COORD]=hold;
  sink.on?.('drain',hold.drain);sink.on?.('close',hold.close);source.on?.('close',hold.close);
  try{source.pause();this.stats.pauses++;this.warn(`backpressure ${this.direction} queue=${queued}B; paused reading temporarily`);}
  catch{this.release(hold,'pause-error');}
 }
 release(hold,reason){
  if(!hold||this.holds.get(hold.source)!==hold)return;
  this.holds.delete(hold.source);
  if(hold.client?.[COORD]===hold)delete hold.client[COORD];
  hold.sink.removeListener?.('drain',hold.drain);hold.sink.removeListener?.('close',hold.close);hold.source.removeListener?.('close',hold.close);
  if(!hold.source.destroyed){try{hold.source.resume();}catch{}}
  this.stats.releases++;
  if(reason==='timeout'){this.stats.timeouts++;this.cooldowns.set(hold.source,Date.now()+this.config.backpressure.retryCooldownMs);this.warn(`backpressure timeout ${this.direction}; reading resumed`);}
 }
 maintain(){
  if(!this.config.enabled)return;
  const now=Date.now();
  for(const h of this.holds.values()){
   if(h.source.destroyed||h.sink.destroyed||h.client?.disconnecting){this.release(h,'close');continue;}
   if(this.direction==='C2S'&&h.client.server?.socket!==h.sink){this.release(h,'switched');continue;}
   if(this.direction==='S2C'&&h.client.server?.socket!==h.source){this.release(h,'switched');continue;}
   if((Number(h.sink.writableLength)||0)<=this.config.backpressure.lowWaterBytes){this.release(h,'low-water');continue;}
   if(now-h.started>=this.config.backpressure.maxPauseMs)this.release(h,'timeout');
  }
 }
 disconnect(client){for(const h of Array.from(this.holds.values()))if(h.client===client)this.release(h,'disconnect');}
 socketClose(socket,client){for(const h of Array.from(this.holds.values()))if(h.source===socket||h.sink===socket||h.client===client)this.release(h,'close');}
 report(){this.log(`flow ${this.direction} active-pauses=${this.holds.size} paused=${this.stats.pauses} resumed=${this.stats.releases} timeout=${this.stats.timeouts} peak-queue=${this.stats.peakQueueBytes}B bytes=${this.stats.bytes}`);}
 snapshot(){return {direction:this.direction,enabled:this.config.enabled,activePauses:this.holds.size,...this.stats};}
 unload(){if(this.maintenanceTimer)clearInterval(this.maintenanceTimer);if(this.summaryTimer)clearInterval(this.summaryTimer);for(const h of Array.from(this.holds.values()))this.release(h,'unload');}
}
module.exports={TransportFlow,parseConfig};
