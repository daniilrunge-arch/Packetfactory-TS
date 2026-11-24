'use strict';

// Terraria packet 1: [UInt16 length][UInt8 type=1][7-bit UTF-8 string length][bytes].
// Read only: NEVER rewrite the client's version without a complete compatible translator.
function readConnectVersion(data) {
  if (!Buffer.isBuffer(data) || data.length < 5 || data[2] !== 1) return null;
  let offset = 3, length = 0, shift = 0;
  for (let index = 0; index < 5; index++) {
    if (offset >= data.length) return null;
    const byte = data[offset++];
    length |= (byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      if (length < 1 || length > 64 || offset + length !== data.length) return null;
      const version = data.toString('utf8', offset, offset + length);
      return /^[A-Za-z0-9._-]{1,64}$/.test(version) ? version : null;
    }
    shift += 7;
  }
  return null;
}

function handleClientPacket(extension, client, packet) {
  if (!extension.config.enabled) return false;
  if (!extension.fastPath) extension.observe('C2S', client, packet, null);
  if (packet && packet.packetType === 1) {
    const version = readConnectVersion(packet.data);
    if (version) {
      const originallyObserved = client?.[Symbol.for('rext.dimensions-mcl.original-terraria-protocol')];
      extension.noteHandshake(client,version==='Terraria326' && originallyObserved ? originallyObserved : version);
    }
  }
  // false = keep the original Dimensions packet flow unchanged.
  return false;
}

module.exports = { readConnectVersion, handleClientPacket };
