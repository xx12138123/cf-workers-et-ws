/**
 * EasyTier WebSocket 中继房间实现
 *
 * 本文件实现了 WebSocket 连接管理、心跳检测和断线清理功能
 * 主要修复了原版存在的"幽灵节点"问题，实现了秒级断线检测和实时设备列表刷新
 *
 * 核心改进：
 * - 心跳机制优化：10秒心跳间隔，25秒超时判定
 * - 主动清理机制：超时后立即触发全局网络清理
 * - 防抖保护：防止重复清理导致的广播风暴
 * - 0值绕过修复：避免从未发过Pong的死连接成为永久幽灵
 * - 双端点 /ws + /ws2 错开断开：规避 Cloudflare Workers 免费 WS 100s 限制
 *
 * @file relay_room.js
 * @version 2.1.0
 */

import { Buffer } from 'buffer';
import { parseHeader, createHeader } from './core/packet.js';
import { PacketType, HEADER_SIZE, MY_PEER_ID, MY_PEER_ID_WS2, EndpointType, serverPeerIdFor } from './core/constants.js';
import { loadProtos } from './core/protos.js';
import { handleHandshake, handlePing, handleForwarding, updateNetworkGroupActivity, removeNetworkGroupActivity } from './core/basic_handlers.js';
import { handleRpcReq, handleRpcResp } from './core/rpc_handler.js';
import { getPeerManager } from './core/peer_manager.js';
import { randomU64String } from './core/crypto.js';

const WS_OPEN = (typeof WebSocket !== 'undefined' && WebSocket.OPEN) ? WebSocket.OPEN : 1;

export class RelayRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.types = loadProtos();
    this.peerManager = getPeerManager();
    this.peerManager.setTypes(this.types);

    // 读取错开断开配置
    this.wsDisconnectMs = Number(env.WS_DISCONNECT_INTERVAL_MS || 90000);
    this.ws2FirstDisconnectMs = Number(env.WS2_FIRST_DISCONNECT_INTERVAL_MS || 45000);
    this.ws2DisconnectMs = Number(env.WS2_DISCONNECT_INTERVAL_MS || 90000);

    // 记录每个客户端 peerId 是否已经至少连接过一次 /ws2。
    // 在 Durable Object 内以普通属性保存（同一 DO 实例存活期间有效；DO 休眠后
    // 重建会丢失，重建后 /ws2 第一次连接会再次按首次 45s 处理，这是可接受的）。
    this.ws2ConnectedPeers = new Set();

    // Restore sockets after hibernation to keep metadata
    this.state.getWebSockets().forEach((ws) => this._restoreSocket(ws));
  }

  async fetch(request) {
    const url = new URL(request.url);
    const endpoint = url.searchParams.get('endpoint') || EndpointType.WS;
    const wsPath = '/' + (endpoint === EndpointType.WS2 ? (this.env.WS2_PATH || 'ws2') : (this.env.WS_PATH || 'ws'));
    // 这里不再严格校验 pathname，只要 Upgrade 正常即可（外层 Worker 已路由）
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected websocket', { status: 400 });
    }

    const pair = new WebSocketPair();
    const server = pair[1];
    const client = pair[0];
    await this.handleSession(server, endpoint);

    return new Response(null, { status: 101, webSocket: client });
  }

  async handleSession(webSocket, endpointType) {
    this.state.acceptWebSocket(webSocket);
    this._initSocket(webSocket, {}, endpointType);
  }

  async webSocketMessage(ws, message) {
    try {
      let buffer = null;
      if (message instanceof ArrayBuffer) {
        buffer = Buffer.from(message);
      } else if (message instanceof Uint8Array) {
        buffer = Buffer.from(message);
      } else if (ArrayBuffer.isView(message) && message.buffer) {
        buffer = Buffer.from(message.buffer);
      } else {
        console.warn('[ws] unsupported message type', typeof message);
        return;
      }
      console.log(`[ws] recv len=${buffer.length} endpoint=${ws.endpointType}`);
      ws.lastSeen = Date.now();
      const header = parseHeader(buffer);
      if (!header) {
        console.error('[ws] parseHeader failed, raw hex=', buffer.toString('hex'));
        return;
      }
      console.log(`[ws] header from=${header.fromPeerId} to=${header.toPeerId} type=${header.packetType} len=${header.len}`);
      const payload = buffer.subarray(HEADER_SIZE);
      switch (header.packetType) {
        case PacketType.HandShake:
          console.log(`[ws] -> handleHandshake payload hex=${payload.toString('hex')}`);
          handleHandshake(ws, header, payload, this.types);
          // 握手成功后，按端点类型启动错开的服务端主动断开定时器
          if (ws.peerId) {
            this.scheduleServerDisconnect(ws);
          }
          break;
        case PacketType.Ping:
          handlePing(ws, header, payload);
          break;
        case PacketType.Pong:
          this._handlePong(ws);
          break;
        case PacketType.RpcReq:
          if (header.toPeerId === undefined || header.toPeerId === null) {
            handleRpcReq(ws, header, payload, this.types);
            break;
          }
          if (header.toPeerId === ws.serverPeerId || header.toPeerId === MY_PEER_ID || header.toPeerId === MY_PEER_ID_WS2) {
            handleRpcReq(ws, header, payload, this.types);
            break;
          }
          handleForwarding(ws, header, buffer, this.types);
          break;
        case PacketType.RpcResp:
          if (header.toPeerId === undefined || header.toPeerId === null || header.toPeerId === ws.serverPeerId || header.toPeerId === MY_PEER_ID || header.toPeerId === MY_PEER_ID_WS2) {
            handleRpcResp(ws, header, payload, this.types);
            break;
          }
          // If toPeerId is not our server peerId, forward to the target peer
          if (header.packetType !== PacketType.Data) {
            console.log(`[ws] -> forward RpcResp type=${header.packetType} from=${header.fromPeerId} to=${header.toPeerId} len=${payload.length}`);
          }
          handleForwarding(ws, header, buffer, this.types);
          break;
        case PacketType.Data:
        default:
          if (header.packetType !== PacketType.Data) {
            console.log(`[ws] -> forward type=${header.packetType} len=${payload.length}`);
          }
          handleForwarding(ws, header, buffer, this.types);
      }
    } catch (e) {
      console.error('relay_room message handling error:', e);
      // 不立即关闭连接，只记录错误
      // 连接稳定性比单个消息处理失败更重要
    }
  }

  async webSocketClose(ws) {
    // 【修复】：加入防抖锁，防止风暴
    if (ws.isCleanedUp) return;
    ws.isCleanedUp = true;

    if (ws.heartbeatInterval) {
      clearInterval(ws.heartbeatInterval);
      ws.heartbeatInterval = null;
    }
    if (ws.disconnectTimer) {
      clearTimeout(ws.disconnectTimer);
      ws.disconnectTimer = null;
    }

    if (ws.peerId) {
      const groupKey = ws.groupKey;
      const removed = this.peerManager.removePeer(ws);
      if (removed) {
        try {
          // 【修复】：强制携带 forceFull: true 广播，强推给所有存活节点
          this.peerManager.broadcastRouteUpdate(this.types, groupKey, null, { forceFull: true });
        } catch (_) { }
      }

      if (groupKey && typeof removeNetworkGroupActivity === 'function') {
        try {
          removeNetworkGroupActivity(groupKey);
        } catch (e) {
          console.error('Error removing network group activity:', e);
        }
      }
    }
  }

  async webSocketError(ws) {
    await this.webSocketClose(ws);
  }

  _initSocket(ws, meta = {}, endpointType = EndpointType.WS) {
    ws.endpointType = meta.endpointType || endpointType;
    ws.serverPeerId = serverPeerIdFor(ws.endpointType);
    ws.peerId = meta.peerId || null;
    ws.groupKey = meta.groupKey || null;
    ws.domainName = meta.domainName || null;
    ws.lastSeen = Date.now();
    ws.lastPingSent = 0;
    // 【修复】：初始化为当前时间，避免 0 值导致的永久免死金牌
    ws.lastPongReceived = Date.now();
    ws.serverSessionId = meta.serverSessionId || randomU64String();
    ws.weAreInitiator = false;
    ws.crypto = { enabled: false };
    ws.heartbeatInterval = null;
    ws.disconnectTimer = null;
    ws.serializeAttachment?.({
      peerId: ws.peerId,
      groupKey: ws.groupKey,
      domainName: ws.domainName,
      serverSessionId: ws.serverSessionId,
      endpointType: ws.endpointType,
    });

    // 启动心跳机制
    this._startHeartbeat(ws);
  }

  _restoreSocket(ws) {
    const meta = ws.deserializeAttachment ? (ws.deserializeAttachment() || {}) : {};
    this._initSocket(ws, meta, meta.endpointType || EndpointType.WS);

    if (ws.peerId && ws.groupKey) {
      this.peerManager.addPeer(ws.peerId, ws);
    }
  }

  _startHeartbeat(ws) {
    if (ws.heartbeatInterval) {
      clearInterval(ws.heartbeatInterval);
    }

    // 缩短超时判定周期，提升列表刷新速度
    const heartbeatInterval = 10000;
    const connectionTimeout = 25000;
    const checkInterval = 5000;

    console.log(`[heartbeat] Starting heartbeat for peer ${ws.peerId} endpoint=${ws.endpointType}`);

    ws.heartbeatInterval = setInterval(() => {
      try {
        if (ws.readyState === WS_OPEN) {
          const now = Date.now();
          if (now - ws.lastPingSent > heartbeatInterval) {
            this._sendPing(ws);
            ws.lastPingSent = now;
          }

          // 【修复】：删掉原版错误的 > 0 判断，超时后主动触发全局网络清理
          if (now - ws.lastPongReceived > connectionTimeout) {
            console.log(`[heartbeat] Connection timeout for peer ${ws.peerId}, forcing cleanup`);
            this.webSocketClose(ws);
            try { ws.close(); } catch(_) {}
            return;
          }
        } else {
          this.webSocketClose(ws);
        }
      } catch (e) {
        console.error('[heartbeat] Error in heartbeat interval:', e);
      }
    }, checkInterval);
  }

  _sendPing(ws) {
    try {
      if (ws.readyState === WS_OPEN) {
        const pingData = Buffer.from('ping');
        const header = createHeader(ws.serverPeerId, ws.peerId, PacketType.Ping, pingData.length);
        ws.send(Buffer.concat([header, pingData]));
        console.log(`[heartbeat] Sent ping to peer ${ws.peerId} from ${ws.serverPeerId}`);
      }
    } catch (e) {
      console.error(`[heartbeat] Failed to send ping to peer ${ws.peerId}:`, e);
    }
  }

  _handlePong(ws) {
    ws.lastPongReceived = Date.now();
    console.log(`[heartbeat] Received pong from peer ${ws.peerId}`);
  }

  /**
   * 在握手完成后启动服务端主动断开定时器。
   * - /ws  : 到 wsDisconnectMs (90s) 时断开
   * - /ws2 : 若该 peer 是首次连 /ws2，则到 ws2FirstDisconnectMs (45s) 断开；
   *          否则到 ws2DisconnectMs (90s) 断开。
   * 这样 /ws 与 /ws2 的断开时间永久错开 45s，保证任意时刻至少有一路连接存活。
   */
  scheduleServerDisconnect(ws) {
    if (ws.disconnectTimer) {
      clearTimeout(ws.disconnectTimer);
    }
    let delay;
    if (ws.endpointType === EndpointType.WS2) {
      const isFirst = !this.ws2ConnectedPeers.has(ws.peerId);
      delay = isFirst ? this.ws2FirstDisconnectMs : this.ws2DisconnectMs;
      this.ws2ConnectedPeers.add(ws.peerId);
      console.log(`[disconnect] /ws2 peer=${ws.peerId} isFirst=${isFirst} will close in ${delay}ms`);
    } else {
      delay = this.wsDisconnectMs;
      console.log(`[disconnect] /ws peer=${ws.peerId} will close in ${delay}ms`);
    }

    ws.disconnectTimer = setTimeout(() => {
      try {
        if (ws.readyState === WS_OPEN) {
          console.log(`[disconnect] server-initiated close for peer=${ws.peerId} endpoint=${ws.endpointType}`);
          // 使用 4000 自定义关闭码，便于客户端识别为"服务端主动断开，应立即重连"
          try { ws.close(4000, 'server-rotate'); } catch (_) {}
        }
      } catch (e) {
        console.error('[disconnect] error during scheduled close:', e);
      }
    }, delay);
  }
}