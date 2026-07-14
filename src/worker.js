// Cloudflare Worker entry for EasyTier WebSocket relay backed by Durable Object
// Module syntax is required for Durable Objects.
import { RelayRoom } from './worker/relay_room';

export { RelayRoom };

// 全局网络组状态（用于管理端点）
const globalNetworkState = {
  networks: new Map(),
  lastUpdated: Date.now()
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const { pathname, searchParams } = url;

    if (pathname === '/healthz') {
      return new Response('ok', { status: 200 });
    }

    // 网络管理端点
    if (pathname === '/admin/networks') {
      return this.handleNetworkAdmin(request, env);
    }

    const wsPath = '/' + (env.WS_PATH || 'ws');
    const ws2Path = '/' + (env.WS2_PATH || 'ws2');
    const isWs = pathname === wsPath || pathname === wsPath + '/';
    const isWs2 = pathname === ws2Path || pathname === ws2Path + '/';
    if (isWs || isWs2) {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket upgrade', { status: 400 });
      }

      const roomId = searchParams.get('room') || 'default';
      // /ws and /ws2 share the same Durable Object so peers on both endpoints
      // can see each other and exchange routing info. We tag the request with
      // the endpoint type via a query param so the DO knows which identity to use.
      const endpointType = isWs2 ? 'ws2' : 'ws';
      const forwardUrl = new URL(request.url);
      if (!searchParams.has('endpoint')) {
        forwardUrl.searchParams.set('endpoint', endpointType);
      }
      const options = env.LOCATION_HINT ? { locationHint: env.LOCATION_HINT } : {};
      const roomStub = env.RELAY_ROOM.get(env.RELAY_ROOM.idFromName(roomId), options);
      return roomStub.fetch(new Request(forwardUrl, request));
    }

    return new Response('Not found', { status: 404 });
  },

  async handleNetworkAdmin(request, env) {
    try {
      const url = new URL(request.url);
      const action = url.searchParams.get('action');
      const networkName = url.searchParams.get('network');

      if (request.method === 'GET') {
        if (action === 'list') {
          // 返回网络组列表（模拟数据，实际应从Durable Object获取）
          const networks = Array.from(globalNetworkState.networks.entries()).map(([name, data]) => ({
            name,
            groups: data.groups || [],
            peerCount: data.peerCount || 0,
            lastActivity: data.lastActivity || Date.now()
          }));

          return new Response(JSON.stringify({
            success: true,
            networks,
            timestamp: Date.now()
          }), {
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid action or method'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });

    } catch (error) {
      return new Response(JSON.stringify({
        success: false,
        error: error.message
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};
