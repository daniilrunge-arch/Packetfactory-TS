'use strict';

function handleServerPacket(extension, server, packet, source) {
  if (!extension.config.enabled) return false;
  // PacketSource.TerrariaServer = 0, PacketSource.Dimensions = 1.
  if (source === 1 && !extension.config.includeSyntheticPackets) return false;
  if (!extension.fastPath && server && server.client) extension.observe('S2C', server.client, packet, source);
  // false = do not consume, drop or modify any server packet.
  return false;
}

module.exports = { handleServerPacket };
