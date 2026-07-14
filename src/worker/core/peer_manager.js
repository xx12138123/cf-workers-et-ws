/**
 * EasyTier 对等节点管理器
 * 
 * 本文件实现了对等节点管理、路由同步和连接位图生成功能
 * 核心改进：引入基于时间戳的全局单调递增版本号，彻底解决路由版本污染问题
 * 
 * 核心修复：
 * - 全局单调递增版本号：防止Worker重启或P2P交叉污染导致的版本回退
 * - 强制版本同步：所有peer使用统一的全局版本号，确保客户端必须接收新路由
 * - 拓扑签名简化：签名只基于拓扑结构本身，不包含错误的独立版本号
 * 
 * @file peer_manager.js
 * @version 2.0.0
 */

import { Buffer } from 'buffer';
import { MY_PEER_ID, MY_PEER_ID_WS2, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String } from './crypto.js';
import { getPeerCenterState, cleanPeerAndSubPeers } from './global_state.js';

const WS_OPEN = 1; // WebSocket.OPEN in CF runtime

function parseIpv4ToU32Be(ip) {
  const parts = String(ip).trim().split('.').map(x => Number(x));
  if (parts.length !== 4 || parts.some(x => !Number.isInteger(x) || x < 0 || x > 255)) {
    throw new Error(`Invalid IPv4: ${ip}`);
  }
  return ((parts[0] << 24) >>> 0) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
}

function mask32FromLen(len) {
  const l = Number(len);
  if (!Number.isFinite(l) || l <= 0) return 0;
  if (l >= 32) return 0xFFFFFFFF >>> 0;
  return (0xFFFFFFFF << (32 - l)) >>> 0;
}

function deriveSameNetworkIpv4(peerAddr, networkLength, myPeerId) {
  const mask = mask32FromLen(networkLength);
  const net = (peerAddr >>> 0) & mask;
  const hostBits = 32 - Number(networkLength);
  if (!Number.isFinite(hostBits) || hostBits <= 1 || hostBits > 30) {
    return null;
  }
  const hostMax = (1 << hostBits) >>> 0;
  const peerHost = (peerAddr >>> 0) & (~mask >>> 0);
  let host = (Number(myPeerId) % 250) + 2;
  if (host >= hostMax) {
    host = (Number(myPeerId) % Math.max(hostMax - 2, 1)) + 1;
  }
  if (host === peerHost) {
    host = (host + 1) % hostMax;
    if (host === 0) host = 1;
  }
  return (net | host) >>> 0;
}

// 生成安全的 32位无符号整数
function randomUint32() {
  return Math.floor(Math.random() * 4294967296);
}

function makeInstId() {
  return {
    part1: randomUint32(),
    part2: randomUint32(),
    part3: randomUint32(),
    part4: randomUint32(),
  };
}

function makeStubPeerInfo(peerId, networkLength) {
  return {
    peerId,
    version: 1,
    lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
    instId: makeInstId(),
    cost: 1,
    hostname: "CF-ETSV",
    easytierVersion: "cf-et-ws",
    featureFlag: { isPublicServer: false, avoidRelayData: false, kcpInput: false, noRelayKcp: false },
    networkLength: Number(networkLength || 24),
    peerRouteId: randomU64String(),
    groups: [],
    udpStunInfo: 1, // 默认设置为 OpenInternet，鼓励 P2P 打洞
  };
}

// 服务器自身信息：/ws 用 MY_PEER_ID，/ws2 用 MY_PEER_ID_WS2，客户端视作两台独立服务器。
function makeServerPeerInfo(peerId) {
  const info = {
    peerId,
    instId: makeInstId(),
    cost: 1,
    version: 1,
    featureFlag: {
      isPublicServer: true,
      avoidRelayData: false,
      kcpInput: false,
      noRelayKcp: false
    },
    networkLength: Number(process.env.EASYTIER_NETWORK_LENGTH || 24),
    easytierVersion: process.env.EASYTIER_VERSION || "cf-et-ws",
    lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
    hostname: process.env.EASYTIER_HOSTNAME || "CF-ETSV",
    udpStunInfo: 1, // 服务器设置为 OpenInternet，支持 P2P 打洞
    peerRouteId: randomU64String(),
    groups: [],
  };

  if (process.env.EASYTIER_IPV4_ADDR) {
    info.ipv4Addr = { addr: parseIpv4ToU32Be(process.env.EASYTIER_IPV4_ADDR) };
  } else if (process.env.EASYTIER_AUTO_IPV4_ADDR === '1') {
    const lastOctet = (Number(peerId) % 250) + 2;
    info.ipv4Addr = { addr: parseIpv4ToU32Be(`10.0.0.${lastOctet}`) };
  }
  return info;
}

export class PeerManager {
  constructor() {
    // groupKey -> Map(peerId -> Set<ws>)
    // 一个客户端 peerId 可能同时通过 /ws 和 /ws2 连接，故用 Set 保存所有活跃连接。
    this.peersByGroup = new Map();
    this.peerInfosByGroup = new Map(); // groupKey -> Map(peerId -> peerInfo)
    this.routeSessions = new Map(); // groupKey -> peerId -> session state
    this.peerConnVersions = new Map(); // groupKey -> peerId -> version
    this.types = null;

    this.allowVirtualIP = false;
    this.ipConfiguredByEnv = !!process.env.EASYTIER_IPV4_ADDR;
    this.netConfiguredByEnv = process.env.EASYTIER_NETWORK_LENGTH !== undefined;
    this.ipAutoAssigned = false;
    // 服务器自身信息，按端点区分：/ws 用 MY_PEER_ID，/ws2 用 MY_PEER_ID_WS2
    this.myInfo = null;
    this.myInfoWs2 = null;
    this.sessionTtlMs = Number(process.env.EASYTIER_SESSION_TTL_MS || 3 * 60 * 1000);
    this.lastSessionCleanup = 0;

    this.pureP2PMode = (process.env.EASYTIER_DISABLE_RELAY === '1');
  }

  setTypes(types) {
    this.types = types;
  }

  ensureMyInfo() {
    if (this.myInfo) return this.myInfo;
    this.myInfo = makeServerPeerInfo(MY_PEER_ID);
    this.myInfo.featureFlag = {
      ...this.myInfo.featureFlag,
      isPublicServer: true,
      avoidRelayData: this.pureP2PMode,
    };
    return this.myInfo;
  }

  ensureMyInfoWs2() {
    if (this.myInfoWs2) return this.myInfoWs2;
    this.myInfoWs2 = makeServerPeerInfo(MY_PEER_ID_WS2);
    this.myInfoWs2.featureFlag = {
      ...this.myInfoWs2.featureFlag,
      isPublicServer: true,
      avoidRelayData: this.pureP2PMode,
    };
    return this.myInfoWs2;
  }

  // 根据连接所属端点返回对应的服务器自身信息
  getServerInfo(ws) {
    return ws && ws.endpointType === 'ws2' ? this.ensureMyInfoWs2() : this.ensureMyInfo();
  }

  bumpMyInfoVersion() {
    const myInfo = this.ensureMyInfo();
    myInfo.version = (myInfo.version || 0) + 1;
    myInfo.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
    const myInfoWs2 = this.ensureMyInfoWs2();
    myInfoWs2.version = (myInfoWs2.version || 0) + 1;
    myInfoWs2.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
  }

  _getPeerConnVersionMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerConnVersions.get(k);
    if (!m && create) {
      m = new Map();
      this.peerConnVersions.set(k, m);
    }
    return m;
  }

  bumpPeerConnVersion(groupKey, peerId) {
    const m = this._getPeerConnVersionMap(groupKey, true);
    const current = m.get(peerId) || 0;
    const next = current + 1;
    m.set(peerId, next);
    return next;
  }

  getPeerConnVersion(groupKey, peerId) {
    const m = this._getPeerConnVersionMap(groupKey, false);
    return m ? (m.get(peerId) || 0) : 0;
  }

  bumpAllPeerConnVersions(groupKey) {
    const allPeers = new Set(this.listPeerIdsInGroup(groupKey));
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) {
      for (const pid of infos.keys()) {
        allPeers.add(pid);
      }
    }
    allPeers.add(MY_PEER_ID);
    allPeers.add(MY_PEER_ID_WS2);
    for (const pid of allPeers) {
      this.bumpPeerConnVersion(groupKey, pid);
    }
  }

  setPublicServerFlag(isPublicServer) {
    const next = !!isPublicServer;
    for (const myInfo of [this.ensureMyInfo(), this.ensureMyInfoWs2()]) {
      const prev = !!(myInfo.featureFlag && myInfo.featureFlag.isPublicServer);
      myInfo.featureFlag = {
        ...myInfo.featureFlag,
        isPublicServer: next,
      };
      if (next !== prev) {
        myInfo.version = (myInfo.version || 0) + 1;
        myInfo.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
      }
    }
  }

  setPureP2PMode(enabled) {
    const next = !!enabled;
    if (next === this.pureP2PMode) return;
    this.pureP2PMode = next;
    for (const myInfo of [this.ensureMyInfo(), this.ensureMyInfoWs2()]) {
      myInfo.featureFlag = {
        ...myInfo.featureFlag,
        avoidRelayData: this.pureP2PMode,
      };
      myInfo.version = (myInfo.version || 0) + 1;
      myInfo.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
    }
  }

  isPureP2PMode() {
    return !!this.pureP2PMode;
  }

  _getPeersMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peersByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peersByGroup.set(k, m);
    }
    return m;
  }

  _getPeerInfosMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerInfosByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peerInfosByGroup.set(k, m);
    }
    return m;
  }

  _getSession(groupKey, peerId, create = false) {
    const now = Date.now();
    if (now - this.lastSessionCleanup > Math.max(30_000, Math.min(this.sessionTtlMs / 2, 120_000))) {
      this.cleanupSessions(now);
    }
    const gk = String(groupKey || '');
    let g = this.routeSessions.get(gk);
    if (!g && create) {
      g = new Map();
      this.routeSessions.set(gk, g);
    }
    if (!g) return null;
    let s = g.get(peerId);
    if (!s && create) {
      s = {
        mySessionId: null,
        dstSessionId: null,
        weAreInitiator: false,
        peerInfoVerMap: new Map(),
        connBitmapVerMap: new Map(),
        foreignNetVer: 0,
        lastTouch: Date.now(),
        lastConnBitmapSig: null,
      };
      g.set(peerId, s);
    }
    if (s) s.lastTouch = Date.now();
    return s;
  }

  cleanupSessions(nowTs = Date.now()) {
    this.lastSessionCleanup = nowTs;
    const ttl = this.sessionTtlMs;
    for (const [gk, m] of this.routeSessions.entries()) {
      for (const [pid, s] of m.entries()) {
        if (nowTs - (s.lastTouch || 0) > ttl) {
          m.delete(pid);
        }
      }
      if (m.size === 0) this.routeSessions.delete(gk);
    }
  }

  onRouteSessionAck(groupKey, peerId, theirSessionId, weAreInitiator) {
    const s = this._getSession(groupKey, peerId, true);
    const isNewSession = s.dstSessionId !== theirSessionId;
    
    if (isNewSession) {
      console.log(`[SessionAck] New session detected for peer ${peerId}, resetting all version info`);
      s.peerInfoVerMap.clear();
      s.connBitmapVerMap.clear();
      s.foreignNetVer = 0;
      s.lastConnBitmapSig = null;
      
      // 强制重置连接版本，确保重连后能获取完整的连接位图
      const connVersions = this._getPeerConnVersionMap(groupKey, true);
      connVersions.set(peerId, 1); // 重置为初始版本
    }
    
    s.dstSessionId = theirSessionId;
    if (typeof weAreInitiator === 'boolean') {
      s.weAreInitiator = weAreInitiator;
    }
    
    console.log(`[SessionAck] Session updated for peer ${peerId}: newSession=${isNewSession}, weAreInitiator=${weAreInitiator}`);
  }

  addPeer(peerId, ws) {
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    const peers = this._getPeersMap(groupKey, true);
    let set = peers.get(peerId);
    const isNewPeer = !set;
    if (!set) {
      set = new Set();
      peers.set(peerId, set);
    }
    set.add(ws);
    if (isNewPeer) {
      this.bumpAllPeerConnVersions(groupKey);
    }
  }

  removePeer(ws) {
    const peerId = ws && ws.peerId;
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    if (!peerId) return false;

    const peers = this._getPeersMap(groupKey, false);
    const set = peers ? peers.get(peerId) : null;
    const wasPresent = !!(set && set.size > 0);
    if (set) {
      set.delete(ws);
      if (set.size === 0) peers.delete(peerId);
    }

    // 仅当该 peerId 已无任何活跃连接时，才真正清理其状态
    const stillConnected = !!(peers && peers.get(peerId) && peers.get(peerId).size > 0);
    if (!stillConnected) {
      // 清理全局状态中的子设备信息
      try {
        cleanPeerAndSubPeers(groupKey, peerId);
      } catch (e) {
        console.warn(`[PeerCleanup] Failed to clean global state for peer ${peerId}:`, e.message);
      }

      const infos = this._getPeerInfosMap(groupKey, false);
      if (infos) infos.delete(peerId);
      const sessions = this.routeSessions.get(groupKey);
      if (sessions) {
        sessions.delete(peerId);
        if (sessions.size === 0) this.routeSessions.delete(groupKey);
      }
      const connVers = this._getPeerConnVersionMap(groupKey, false);
      if (connVers) connVers.delete(peerId);

      if (wasPresent && peers && peers.size > 0) {
        this.bumpAllPeerConnVersions(groupKey);
      }

      if (peers && peers.size === 0) {
        this.peersByGroup.delete(groupKey);
        this.peerInfosByGroup.delete(groupKey);
        this.peerConnVersions.delete(groupKey);
      }
    }

    console.log(`[PeerCleanup] Removed connection for peer ${peerId} from group ${groupKey}, stillConnected=${stillConnected}`);
    return true;
  }

  // 返回该 peerId 的任意一个活跃连接（优先返回与 excludeWs 不同的端点，以分散流量）
  getPeerWs(peerId, groupKey, excludeWs = null) {
    const peers = this._getPeersMap(groupKey, false);
    const set = peers ? peers.get(peerId) : null;
    if (!set || set.size === 0) return undefined;
    let fallback = undefined;
    for (const ws of set) {
      if (ws.readyState === WS_OPEN) {
        if (ws !== excludeWs) return ws;
        if (!fallback) fallback = ws;
      }
    }
    return fallback;
  }

  // 返回该 peerId 的所有活跃连接
  getPeerWsAll(peerId, groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    const set = peers ? peers.get(peerId) : null;
    if (!set) return [];
    const out = [];
    for (const ws of set) {
      if (ws.readyState === WS_OPEN) out.push(ws);
    }
    return out;
  }

  listPeerIdsInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.keys()) : [];
  }

  listPeersInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    if (!peers) return [];
    // 返回 [peerId, representativeWs] 对（取每个 Set 中的第一个连接）
    const out = [];
    for (const [peerId, set] of peers.entries()) {
      const ws = set.values().next().value;
      if (ws) out.push([peerId, ws]);
    }
    return out;
  }

  updatePeerInfo(groupKey, peerId, info) {
    const infos = this._getPeerInfosMap(groupKey, true);
    const isNew = !infos.has(peerId);
    infos.set(peerId, info);
    if (isNew) {
      this.bumpAllPeerConnVersions(groupKey);
    }
  }

  broadcastRouteUpdate(types, groupKey, excludePeerId, opts = {}) {
    const forceFull = opts.forceFull !== undefined ? !!opts.forceFull : true;
    const pushForSet = (peers) => {
      if (!peers) return;
      for (const [peerId, set] of peers.entries()) {
        if (peerId === excludePeerId) continue;
        for (const ws of set) {
          if (ws.readyState === WS_OPEN) {
            this.pushRouteUpdateTo(peerId, ws, types, { forceFull });
          }
        }
      }
    };
    if (groupKey !== undefined) {
      pushForSet(this._getPeersMap(groupKey, false));
      return;
    }
    for (const peers of this.peersByGroup.values()) {
      pushForSet(peers);
    }
  }

  pushRouteUpdateTo(targetPeerId, ws, types, opts = {}) {
    const forceFull = !!opts.forceFull;
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    const session = this._getSession(groupKey, targetPeerId, true);
    // 该连接所属端点的服务器 peerId 与信息
    const myPeerId = (ws && ws.serverPeerId) ? ws.serverPeerId : MY_PEER_ID;
    const myInfo = this.getServerInfo(ws);
    if (!ws.serverSessionId) {
      ws.serverSessionId = randomU64String();
    }
    session.mySessionId = ws.serverSessionId;
    const forceFullLocal = forceFull || !session.dstSessionId;

    // 收集所有相关的 peer，包括子设备
    const allPeers = new Set(this.listPeerIdsInGroup(groupKey));
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) {
      for (const pid of infos.keys()) {
        allPeers.add(pid);
      }
    }

    // 添加全局 peer 映射中的子设备
    try {
      const globalState = getPeerCenterState(groupKey);
      for (const [peerId, peerInfo] of globalState.globalPeerMap.entries()) {
        allPeers.add(Number(peerId));
        if (peerInfo.directPeers) {
          for (const subPeerId of Object.keys(peerInfo.directPeers)) {
            allPeers.add(Number(subPeerId));
          }
        }
      }
    } catch (e) {
      console.warn(`Failed to get global peer state for group ${groupKey}:`, e.message);
    }

    allPeers.add(targetPeerId);
    // 两台服务器节点都应出现在路由拓扑中
    allPeers.add(MY_PEER_ID);
    allPeers.add(MY_PEER_ID_WS2);
    const relevantPeers = [myPeerId, MY_PEER_ID, MY_PEER_ID_WS2, ...Array.from(allPeers).filter(p => p !== myPeerId && p !== MY_PEER_ID && p !== MY_PEER_ID_WS2).sort((a, b) => Number(a) - Number(b))];
    const defaultNetLen = myInfo.networkLength || 24;

    console.log(`[RouteUpdate] Pushing route update to ${targetPeerId} via ${myPeerId} with ${relevantPeers.length} peers (including sub-peers)`);

    const peerInfosItems = [];
    for (const pid of relevantPeers) {
      let info;
      if (pid === MY_PEER_ID) {
        info = this.ensureMyInfo();
      } else if (pid === MY_PEER_ID_WS2) {
        info = this.ensureMyInfoWs2();
      } else {
        info = this._getPeerInfosMap(groupKey, false)?.get(pid);
      }

      // 只推送实际存在的 peer 信息，避免为不存在的子设备创建 stub 信息
      if (!info && pid !== MY_PEER_ID && pid !== MY_PEER_ID_WS2) {
        // 检查是否是全局状态中记录的子设备
        try {
          const globalState = getPeerCenterState(groupKey);
          const isKnownSubPeer = Array.from(globalState.globalPeerMap.values()).some(peerInfo =>
            peerInfo.directPeers && String(pid) in peerInfo.directPeers
          );

          if (!isKnownSubPeer) {
            console.log(`[RouteUpdate] Skipping unknown sub-peer ${pid} in route update`);
            continue; // 跳过未知的子设备
          }
        } catch (e) {
          console.warn(`[RouteUpdate] Failed to check global state for peer ${pid}:`, e.message);
          continue; // 出错时跳过
        }

        // 对于已知的子设备，创建临时信息但不保存到本地映射
        info = makeStubPeerInfo(pid, defaultNetLen);
        console.log(`[RouteUpdate] Created temporary info for known sub-peer ${pid}`);
      }

      if (!info) continue; // 确保 info 存在

      const version = info && info.version ? info.version : 1;
      const prev = forceFullLocal ? 0 : (session.peerInfoVerMap.get(pid) || 0);

      // 强制推送或版本变化时包含该 peer 信息
      if (forceFullLocal || version > prev) {
        peerInfosItems.push(info);
        session.peerInfoVerMap.set(pid, version);
        console.log(`[RouteUpdate] Including peer ${pid} in update, forceFull=${forceFullLocal}, version=${version} > prev=${prev}`);
      }
    }

    let connBitmap = null;
    if (relevantPeers.length > 0) {
      const connVersions = this._getPeerConnVersionMap(groupKey, true);
      const peerIdVersions = relevantPeers.map((pid) => {
        const existing = connVersions.get(pid) || 1;
        return { peerId: pid, version: existing };
      });
      const N = peerIdVersions.length;
      const bitmapSize = Math.ceil((N * N) / 8);
      const bitmap = new Uint8Array(bitmapSize);
      const idxByPeerId = new Map();
      for (let i = 0; i < peerIdVersions.length; i++) {
        idxByPeerId.set(peerIdVersions[i].peerId, i);
      }
      const setBit = (row, col) => {
        const idx = row * N + col;
        bitmap[Math.floor(idx / 8)] |= (1 << (idx % 8));
      };

      // 设置所有 peer 之间的连接性（全连接拓扑）
      // 这样所有 peer 都会尝试进行 P2P 打洞
      for (let i = 0; i < peerIdVersions.length; i++) {
        for (let j = 0; j < peerIdVersions.length; j++) {
          setBit(i, j);
        }
      }

      console.log(`[ConnBitmap] Created full-mesh connectivity for ${peerIdVersions.length} peers`);

      // --- 替换开始 ---
      // 【核心修复】：引入基于时间戳的全局单调递增版本号，防止 Worker 重启或 P2P 交叉污染导致的版本回退
      if (typeof this.globalNetworkVersion === 'undefined') {
          // 初始化为当前秒数 (截断防止溢出 u32)，确保它比客户端当前所有的旧缓存版本都要大
          this.globalNetworkVersion = Math.floor(Date.now() / 1000) % 2000000000;
      }

      const bitmapBuf = Buffer.from(bitmap);
      // 签名不再包含本地错误的独立版本号，只根据拓扑结构本身计算
      const sig = `${peerIdVersions.map(p => p.peerId).join(',')}|${bitmapBuf.toString('hex')}`;

      // 只要网络拓扑发生任何变动，或者强制推送时，全局版本号 +1
      if (forceFullLocal || sig !== session.lastConnBitmapSig) {
          this.globalNetworkVersion += 1;
          session.lastConnBitmapSig = sig;
          console.log(`[ConnBitmap] Topology changed, global version bumped to: ${this.globalNetworkVersion}`);
      }

      const currentVersion = this.globalNetworkVersion;
      // 将所有 peer 的版本号强制刷为最新的全局版本号
      for (let i = 0; i < peerIdVersions.length; i++) {
          peerIdVersions[i].version = currentVersion;
      }

      connBitmap = { peerIds: peerIdVersions, bitmap: bitmapBuf, version: currentVersion };
      // --- 替换结束 ---
    }

    const foreignNetworkInfos = (() => {
      const mode = (process.env.EASYTIER_HANDSHAKE_MODE || 'foreign').toLowerCase();
      if (mode === 'same' || mode === 'same_network') return null;
      const version = session.foreignNetVer + 1;
      session.foreignNetVer = version;
      return {
        infos: [{
          key: {
            peerId: myPeerId,
            networkName: process.env.EASYTIER_PUBLIC_SERVER_NETWORK_NAME || 'dev-websocket-relay'
          },
          value: {
            foreignPeerIds: Array.from(allPeers),
            lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
            version,
            // 【关键修复 6】：必须严格传入 32 字节的全 0 Buffer，官方 proto 定义这里是 bytes network_secret_digest
            networkSecretDigest: Buffer.alloc(32),
            myPeerIdForThisNetwork: myPeerId
          }
        }]
      };
    })();

    const t = this.types;
    if (!t) {
      throw new Error('PeerManager types not set');
    }
    const rawPeerInfos = peerInfosItems.length > 0
      ? peerInfosItems.map(info => t.RoutePeerInfo.encode(info).finish())
      : null;

    const reqPayload = {
      myPeerId,
      mySessionId: ws.serverSessionId,
      isInitiator: !!ws.weAreInitiator,
      peerInfos: peerInfosItems.length > 0 ? { items: peerInfosItems } : null,
      rawPeerInfos: rawPeerInfos,
      connBitmap: connBitmap,
      foreignNetworkInfos: foreignNetworkInfos
    };

    const reqBytes = t.SyncRouteInfoRequest.encode(reqPayload).finish();
    const rpcRequestPayload = { request: reqBytes, timeoutMs: 5000 };
    const rpcRequestBytes = t.RpcRequest.encode(rpcRequestPayload).finish();

    const rpcReqPacket = {
      fromPeer: myPeerId,
      toPeer: targetPeerId,
      transactionId: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
      descriptor: {
        domainName: ws.domainName || "public_server",
        protoName: 'OspfRouteRpc',
        serviceName: 'OspfRouteRpc',
        methodIndex: process.env.EASYTIER_OSPF_ROUTE_METHOD_INDEX ? Number(process.env.EASYTIER_OSPF_ROUTE_METHOD_INDEX) : 1
      },
      body: rpcRequestBytes,
      isRequest: true,
      totalPieces: 1,
      pieceIdx: 0,
      traceId: 0,
      compressionInfo: { algo: 1, acceptedAlgo: 1 }
    };

    const rpcPacketBytes = t.RpcPacket.encode(rpcReqPacket).finish();
    try {
      ws.send(wrapPacket(createHeader, myPeerId, targetPeerId, PacketType.RpcReq, rpcPacketBytes, ws));
    } catch (e) {
      // ignore
    }
  }
}

let peerManagerInstance = null;
export function getPeerManager() {
  if (!peerManagerInstance) {
    peerManagerInstance = new PeerManager();
  }
  return peerManagerInstance;
}
