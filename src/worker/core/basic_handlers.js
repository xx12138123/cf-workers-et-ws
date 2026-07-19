/**
 * EasyTier 基础消息处理器
 * 
 * 本文件实现了握手处理、私有网络支持和网络组管理功能
 * 核心改进：实现私有模式拦截和更好的客户端兼容性
 * 
 * 核心功能：
 * - 私有网络支持：通过环境变量配置私有网络名
 * - 握手协议优化：移除features字段提升兼容性
 * - 网络组管理：支持多密码和网络组活动状态跟踪
 * 
 * @file basic_handlers.js
 * @version 2.0.0
 */

import { MAGIC, VERSION, MY_PEER_ID, MY_PEER_ID_WS2, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { getPeerManager } from './peer_manager.js';
import { wrapPacket, randomU64String } from './crypto.js';

const WS_OPEN = (typeof WebSocket !== 'undefined' && WebSocket.OPEN) ? WebSocket.OPEN : 1;

// 服务器侧默认的 feature_flag，所有字段显式给出，避免 proto 缺字段导致客户端误判。
// 关键：disable_p2p=false（服务器不禁用 P2P），need_p2p=false（服务器不主动 P2P）。
function makeServerDefaultFeatureFlag() {
  return {
    isPublicServer: true,
    avoidRelayData: false,
    kcpInput: false,
    noRelayKcp: false,
    supportConnListSync: false,
    quicInput: false,
    noRelayQuic: false,
    isCredentialPeer: false,
    needP2p: false,
    disableP2p: false,
    ipv6PublicAddrProvider: false,
  };
}

// 握手阶段为客户端构造初始 peer_info。此处字段需尽量完整，因为握手后会立即
// 向其他客户端推送路由更新。feature_flag 必须显式给出 disable_p2p=false，
// 否则 proto 默认值与"字段缺失"无法区分，对端可能误判为 disable_p2p=true 而拒绝中转。
function makeInitialClientPeerInfo(peerId, networkLength) {
  return {
    peerId,
    version: 1,
    lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
    instId: { part1: 0, part2: 0, part3: 0, part4: 0 },
    cost: 1,
    hostname: '',
    easytierVersion: '',
    featureFlag: {
      isPublicServer: false,
      avoidRelayData: false,
      kcpInput: false,
      noRelayKcp: false,
      supportConnListSync: false,
      quicInput: false,
      noRelayQuic: false,
      isCredentialPeer: false,
      needP2p: false,
      disableP2p: false,
      ipv6PublicAddrProvider: false,
    },
    networkLength: Number(networkLength || 24),
    peerRouteId: randomU64String(),
    groups: [],
    udpStunInfo: 0, // Unknown: 等待客户端 SyncRouteInfo 上报真实值
  };
}

// 支持多密码：每个网络名称可以对应多个密码摘要
const networkDigestRegistry = new Map(); // networkName -> Set of digests
const networkGroups = new Map(); // networkName:digest -> group metadata

// 网络组管理功能
function updateNetworkGroupActivity(groupKey) {
  const group = networkGroups.get(groupKey);
  if (group) {
    group.lastActivity = Date.now();
    group.peerCount = (group.peerCount || 0) + 1;
  }
}

function removeNetworkGroupActivity(groupKey) {
  const group = networkGroups.get(groupKey);
  if (group) {
    group.peerCount = Math.max(0, (group.peerCount || 1) - 1);
    
    // 如果网络组没有活跃的对等节点，可以清理（可选）
    if (group.peerCount === 0 && Date.now() - group.lastActivity > 24 * 60 * 60 * 1000) {
      // 24小时无活动，清理网络组
      networkGroups.delete(groupKey);
      console.log(`Cleaned up inactive network group: ${groupKey}`);
    }
  }
}

function getNetworkGroupsByNetwork(networkName) {
  const groups = [];
  for (const [groupKey, group] of networkGroups.entries()) {
    if (groupKey.startsWith(`${networkName}:`)) {
      groups.push({
        groupKey,
        ...group
      });
    }
  }
  return groups;
}

function handleHandshake(ws, header, payload, types) {
  try {
    const req = types.HandshakeRequest.decode(payload);

    if (req.magic !== MAGIC) {
      ws.close();
      return;
    }

    const clientNetworkName = req.networkName || '';
    
    // 【新增功能】：私有模式拦截
    // 如果 Worker 配置了私有网络名，且客户端请求的网络名不一致，直接拒绝连接
    const privateNetworkName = process.env.EASYTIER_NETWORK_NAME || '';
    if (privateNetworkName && clientNetworkName !== privateNetworkName) {
      console.error(`[Private Mode] Rejected: Expected ${privateNetworkName}, got ${clientNetworkName}`);
      ws.close(1008, "Network name mismatch");
      return;
    }

    // 根据是否配置了私有网络名，判断是否为公开服务器
    const isPublicServer = !privateNetworkName;
    const serverNetworkName = privateNetworkName || process.env.EASYTIER_PUBLIC_SERVER_NETWORK_NAME || 'public_server';

    // ... (中间的 networkDigestRegistry 等逻辑保持原版不变) ...

    const clientDigest = req.networkSecretDigrest ? Buffer.from(req.networkSecretDigrest) : Buffer.alloc(0);
    const digestHex = clientDigest.toString('hex');
    
    // 支持多密码：检查该网络名称下是否已存在此密码摘要
    let existingDigests = networkDigestRegistry.get(clientNetworkName);
    if (!existingDigests) {
      existingDigests = new Set();
      networkDigestRegistry.set(clientNetworkName, existingDigests);
    }
    
    // 如果密码摘要不为空且不在现有摘要集合中，则创建新的网络组
    if (digestHex.length > 0 && !existingDigests.has(digestHex)) {
      existingDigests.add(digestHex);
      console.log(`Adding new digest for network "${clientNetworkName}": ${digestHex}`);
    }
    
    // 生成网络组键：网络名称:密码摘要
    const groupKey = `${clientNetworkName}:${digestHex}`;
    
    // 初始化网络组元数据（如果不存在）
    if (!networkGroups.has(groupKey)) {
      networkGroups.set(groupKey, {
        createdAt: Date.now(),
        peerCount: 0,
        lastActivity: Date.now()
      });
      console.log(`Created new network group: ${groupKey}`);
    }

    ws.domainName = clientNetworkName;

    // 本连接的服务器 peerId：/ws 用 MY_PEER_ID，/ws2 用 MY_PEER_ID_WS2，客户端视作两台独立服务器
    const myPeerId = ws.serverPeerId || MY_PEER_ID;

    // 【关键修复 5】：移除 features 字段，防止官方客户端严格校验导致拒收
    const respPayload = {
      magic: MAGIC,
      myPeerId,
      version: VERSION,
      networkName: serverNetworkName,
      networkSecretDigrest: new Uint8Array(32) // 注意这里官方 proto 拼写是 Digrest
    };

    console.log(`Handshake response payload:`, {
      magic: respPayload.magic,
      myPeerId: respPayload.myPeerId,
      version: respPayload.version,
      networkName: respPayload.networkName,
      endpoint: ws.endpointType,
      networkSecretDigrestLength: respPayload.networkSecretDigrest ? respPayload.networkSecretDigrest.length : 0
    });

    ws.groupKey = groupKey;
    ws.peerId = req.myPeerId;
    const pm = getPeerManager();
    pm.addPeer(req.myPeerId, ws);

    // 更新网络组活动状态
    updateNetworkGroupActivity(groupKey);
    // 使用完整的初始 peer_info，确保 feature_flag 等字段不缺失，
    // 避免其他客户端因 proto 字段缺失而误判 disable_p2p 状态。
    pm.updatePeerInfo(ws.groupKey, req.myPeerId, makeInitialClientPeerInfo(req.myPeerId, process.env.EASYTIER_NETWORK_LENGTH));
    // 【修改】：使用动态的公开服务器标志
    pm.setPublicServerFlag(isPublicServer);
    ws.crypto = { enabled: false };

    const respBuffer = types.HandshakeRequest.encode(respPayload).finish();
    const respHeader = createHeader(myPeerId, req.myPeerId, PacketType.HandShake, respBuffer.length);
    
    // 改进发送逻辑：添加延迟确保客户端准备好接收
    setTimeout(() => {
      try {
        // 检查连接状态
        if (ws.readyState !== WS_OPEN) {
          console.error(`WebSocket not open when sending handshake response to ${req.myPeerId}, state: ${ws.readyState}`);
          return;
        }
        
        ws.send(Buffer.concat([respHeader, Buffer.from(respBuffer)]));
        console.log(`Handshake response sent to peer ${req.myPeerId}, payload length: ${respBuffer.length}`);
      } catch (sendError) {
        console.error(`Failed to send handshake response to ${req.myPeerId}:`, sendError);
        // 不立即关闭连接，让心跳机制处理
      }
    }, 10); // 10ms延迟确保客户端准备好
    
    if (!ws.serverSessionId) {
      ws.serverSessionId = randomU64String();
    }
    if (ws.weAreInitiator === undefined) {
      ws.weAreInitiator = false;
    }

    setTimeout(() => {
      try {
        if (ws.readyState === WS_OPEN) {
          const pm = getPeerManager();
          
          // 为新设备推送完整的路由信息
          pm.pushRouteUpdateTo(req.myPeerId, ws, types, { forceFull: true });
          
          // 为所有现有设备广播路由更新，包括新设备
          // 确保所有设备都能获得最新的连接位图
          pm.broadcastRouteUpdate(types, ws.groupKey, null, { forceFull: true });
          
          console.log(`[Handshake] Initial route updates completed for peer ${req.myPeerId}`);
        }
      } catch (e) {
        console.error(`Failed to push initial route update to ${req.myPeerId}:`, e.message);
      }
    }, 50);

  } catch (e) {
    console.error('Handshake error:', e);
    // 改进错误处理：只在严重错误时关闭连接
    if (e.message && e.message.includes('decode') || e.message.includes('Invalid')) {
      ws.close();
    }
    // 其他错误不关闭连接，让心跳机制处理
  }
}

function handlePing(ws, header, payload) {
  const myPeerId = ws.serverPeerId || MY_PEER_ID;
  const msg = wrapPacket(createHeader, myPeerId, header.fromPeerId, PacketType.Pong, payload, ws);
  ws.send(msg);
}

function handleForwarding(sourceWs, header, fullMessage, types) {
  const targetPeerId = header.toPeerId;
  const pm = getPeerManager();
  // 该 peerId 可能同时有 /ws 与 /ws2 两条连接，任选一条可用连接转发（优先非当前连接）
  const targetWs = pm.getPeerWs(targetPeerId, sourceWs && sourceWs.groupKey, sourceWs);

  if (targetWs && targetWs.readyState === WS_OPEN) {
    const srcGroup = sourceWs && sourceWs.groupKey;
    const dstGroup = targetWs && targetWs.groupKey;
    if (srcGroup && dstGroup && srcGroup !== dstGroup) {
      return;
    }
    try {
      targetWs.send(fullMessage);
    } catch (e) {
      console.error(`Forward to ${targetPeerId} failed: ${e.message}`);
      pm.removePeer(targetWs);
      try {
        pm.broadcastRouteUpdate(types, srcGroup);
      } catch (err) {
        console.error(`Broadcast after forward failure failed: ${err.message}`);
      }
    }
  } else {
  }
}

export {
  handleHandshake,
  handlePing,
  handleForwarding,
  updateNetworkGroupActivity,
  removeNetworkGroupActivity,
  getNetworkGroupsByNetwork
};
