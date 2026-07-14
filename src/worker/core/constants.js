export const MAGIC = 0xd1e1a5e1;
export const VERSION = 1;
export const MY_PEER_ID = 10000001; // Server Peer ID for /ws
export const MY_PEER_ID_WS2 = 10000002; // Server Peer ID for /ws2 (treated as an independent server)
export const HEADER_SIZE = 16;

// Endpoint types
export const EndpointType = {
  WS: 'ws',
  WS2: 'ws2',
};

// Resolve the server peerId for a given endpoint / ws.
export function serverPeerIdFor(endpointType) {
  return endpointType === EndpointType.WS2 ? MY_PEER_ID_WS2 : MY_PEER_ID;
}

export const PacketType = {
  Invalid: 0,
  Data: 1,
  HandShake: 2,
  RoutePacket: 3, // deprecated
  Ping: 4,
  Pong: 5,
  TaRpc: 6, // deprecated
  Route: 7, // deprecated
  RpcReq: 8,
  RpcResp: 9,
  ForeignNetworkPacket: 10,
  KcpSrc: 11,
  KcpDst: 12,
};
