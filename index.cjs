'use strict';

// Original Dimensions 7.2.0 ESM extension loader accepts CommonJS index.cjs,
// requiring a .default export that is a constructor (new Extension(logging)).
const fs = require('node:fs');
const path = require('node:path');
const { readConnectVersion, handleClientPacket } = require('./client.cjs');
const { handleServerPacket } = require('./server.cjs');
const { TransportFlow } = require('./transport.cjs');

const DEFAULTS = Object.freeze({
  enabled: true, logHandshake: true, logDisconnect: false, logSummary: true,
  summaryIntervalSeconds: 60, summaryTopPacketCount: 12,
  includeSyntheticPackets: false, warnPacketsPerSecondPerClient: 1000,
  warnCooldownSeconds: 60, logPlayerNames: false, logIPAddresses: false,
  snapshot: { enabled: false, path: './logs/packetfactory-ts-status.json' },
  transport: {},
  performance: { highConcurrency: false }
});

function loadConfig() {
  const file = path.join(__dirname, 'config.json');
  const user = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : {};
  const cfg = { ...DEFAULTS, ...user, snapshot: { ...DEFAULTS.snapshot, ...(user.snapshot || {}) }, transport: user.transport || {}, performance: { ...DEFAULTS.performance, ...(user.performance || {}) } };
  cfg.summaryIntervalSeconds = Math.max(5, Math.min(3600, Number(cfg.summaryIntervalSeconds) || 60));
  cfg.summaryTopPacketCount = Math.max(1, Math.min(64, Number(cfg.summaryTopPacketCount) || 12));
  cfg.warnPacketsPerSecondPerClient = Math.max(100, Number(cfg.warnPacketsPerSecondPerClient) || 1000);
  cfg.warnCooldownSeconds = Math.max(10, Number(cfg.warnCooldownSeconds) || 60);
  return cfg;
}

class PacketfactoryTS {
  constructor(logging) {
    this.name = 'Packetfactory-TS';
    this.version = 'v2.0.0-326-329-observer';
    this.author = 'Rext';
    this.reloadable = false;
    this.logging = logging || console;
    this.config = loadConfig();
    this.fastPath = this.config.performance.highConcurrency === true;
    this.flow = new TransportFlow(this.logging, { ...this.config.transport, enabled: this.config.enabled && this.config.transport.enabled !== false }, 'C2S', 'Packetfactory-TS');
    this.sessions = new Map(); // explicitly removed on disconnect
    this.nextSessionId = 1;
    this.startedAt = Date.now();
    this.total = { C2S: { packets: 0, bytes: 0, types: new Map() }, S2C: { packets: 0, bytes: 0, types: new Map() } };
    this.interval = { C2S: { packets: 0, bytes: 0, types: new Map() }, S2C: { packets: 0, bytes: 0, types: new Map() } };
    this.priorPacketHandlers = {
      clientHandler: { handlePacket: (client, packet) => handleClientPacket(this, client, packet) },
      serverHandler: { handlePacket: (server, packet, source) => handleServerPacket(this, server, packet, source) }
    };
    if (this.config.enabled) {
      this._info('loaded: PC/mobile 326 native; 329 telemetry enabled (MCL owns experimental per-client bridging); never rewrites 326 frames');
      if (this.config.logSummary || this.config.snapshot.enabled) {
        this.timer = setInterval(() => this.flush(), this.config.summaryIntervalSeconds * 1000);
        if (typeof this.timer.unref === 'function') this.timer.unref();
      }
    }
  }

  _info(message) {
    const line = `[Packetfactory-TS] ${message}`;
    try { if (typeof this.logging.info === 'function') this.logging.info(line); else console.log(line); }
    catch (_) { /* packet hooks must never crash the proxy for a logging failure */ }
  }

  _session(client) {
    let session = this.sessions.get(client);
    if (!session) {
      session = { id: this.nextSessionId++, startedAt: Date.now(), version: null, second: -1,
        inSecond: 0, lastRateWarningAt: 0, packets: { C2S: 0, S2C: 0 } };
      this.sessions.set(client, session);
    }
    return session;
  }

  _display(client, session) {
    const parts = [`session=${session.id}`];
    if (this.config.logPlayerNames && typeof client?.player?.name === 'string')
      parts.push(`name=${JSON.stringify(client.player.name.slice(0, 32))}`);
    if (this.config.logIPAddresses && client?.ip)
      parts.push(`ip=${String(client.ip).slice(0, 64)}`);
    return parts.join(' ');
  }

  noteHandshake(client, version) {
    const session = this._session(client);
    if (session.version === version) return;
    session.version = version;
    if (!this.config.logHandshake) return;
    let support = 'unknown';
    if (version === 'Terraria329') support = '1.4.6 mobile/PC (329) bridge handled ONLY by Dimensions-MCL; unverified in live server';
    else if (version === 'Terraria326') support = 'protocol-326 (captured PC-format)';
    else if (version === 'Terraria319') support = '1.4.5.6 protocol-319 (experimental packet translation)';
    else if (version === 'Terraria325') support = '1.4.5.7 protocol-325 candidate (experimental packet translation)';
    else if (version === 'Terraria155' || version === 'Terraria156') support = 'legacy-mobile-protocol (NOT translated)';
    this._info(`connect ${this._display(client, session)} version=${version} classification=${support}`);
  }

  observe(direction, client, packet, source) {
    // No packet counters in high-concurrency mode. Packet payloads remain untouched.
    // Every network packet still traverses original Dimensions handlers.
    if (this.fastPath) return;
    if (!packet || !Buffer.isBuffer(packet.data) || !client) return;
    const data = packet.data;
    const type = Number(packet.packetType);
    if (!Number.isInteger(type) || type < 0 || type > 255) return;
    const session = this._session(client);
    // Only header, type and aggregate sizes are stored. NO full packet payload is logged.
    for (const target of [this.total[direction], this.interval[direction]]) {
      target.packets++;
      target.bytes += data.length;
      target.types.set(type, (target.types.get(type) || 0) + 1);
    }
    session.packets[direction]++;
    const now = Date.now();
    const second = Math.floor(now / 1000);
    if (second !== session.second) { session.second = second; session.inSecond = 0; }
    session.inSecond++;
    if (session.inSecond === this.config.warnPacketsPerSecondPerClient &&
        now - session.lastRateWarningAt >= this.config.warnCooldownSeconds * 1000) {
      session.lastRateWarningAt = now;
      this._info(`high-packet-rate ${this._display(client, session)} >=${session.inSecond}/s; observe-only`);
    }
  }

  socketConnectPostHandler(socket) { this.flow.socketConnect(socket); }
  clientConnectEvent(client) { this.flow.clientConnect(client); }
  clientDisconnectEvent(client) { this.flow.disconnect(client); }
  sendPacketToServerEvent(server, buf) { this.flow.toServer(server, buf); }

  socketClosePostHandler(_socket, client) {
    this.flow.socketClose(_socket, client);
    if (!client) return;
    const session = this.sessions.get(client);
    if (!session) return;
    if (this.config.logDisconnect)
      this._info(`disconnect ${this._display(client, session)} c2s=${session.packets.C2S} s2c=${session.packets.S2C}`);
    this.sessions.delete(client);
  }

  _serializeCounter(counter) {
    return {
      packets: counter.packets,
      bytes: counter.bytes,
      top: [...counter.types.entries()].sort((a, b) => b[1] - a[1]).slice(0, this.config.summaryTopPacketCount)
        .map(([packetId, count]) => ({ packetId, count }))
    };
  }

  snapshot() {
    return {
      extension: this.name, version: this.version,
      observeOnly: true, supportsMobileTranslation: false, highConcurrency: this.fastPath,
      countersAvailable: !this.fastPath,
      since: new Date(this.startedAt).toISOString(), timestamp: new Date().toISOString(),
      activeSessions: this.sessions.size,
      transport: this.flow.snapshot(),
      C2S: this._serializeCounter(this.total.C2S), S2C: this._serializeCounter(this.total.S2C)
    };
  }

  flush() {
    const c = this._serializeCounter(this.interval.C2S), s = this._serializeCounter(this.interval.S2C);
    if (this.config.logSummary && (c.packets || s.packets)) {
      const summarize = (v) => v.top.slice(0, 6).map(x => `${x.packetId}:${x.count}`).join(',');
      this._info(`interval active=${this.sessions.size} C2S=${c.packets}/${c.bytes}B [${summarize(c)}] S2C=${s.packets}/${s.bytes}B [${summarize(s)}]`);
    }
    this.interval = { C2S: { packets: 0, bytes: 0, types: new Map() }, S2C: { packets: 0, bytes: 0, types: new Map() } };
    if (this.config.snapshot.enabled) {
      try {
        const full = path.resolve(process.cwd(), this.config.snapshot.path);
        fs.mkdirSync(path.dirname(full), { recursive: true });
        const temporary = full + '.tmp';
        fs.writeFileSync(temporary, JSON.stringify(this.snapshot(), null, 2) + '\n');
        fs.renameSync(temporary, full);
      } catch (e) { this._info(`snapshot-write-error: ${e.message}`); }
    }
  }

  load(_storage) { /* not restoring connection state across reloads */ }

  unload() {
    this.flow.unload();
    if (this.timer) clearInterval(this.timer);
    if (this.config.enabled) this.flush();
    this.sessions.clear();
    return undefined;
  }
}

module.exports.default = PacketfactoryTS;
module.exports.PacketfactoryTS = PacketfactoryTS;
module.exports.readConnectVersion = readConnectVersion;
