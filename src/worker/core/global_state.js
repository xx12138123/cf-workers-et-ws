// 全局状态管理器，避免循环依赖
import { MY_PEER_ID, MY_PEER_ID_WS2 } from './constants.js';

// 全局 peer 中心状态
const peerCenterStateByGroup = new Map();
const PEER_CENTER_TTL_MS = Number(process.env.EASYTIER_PEER_CENTER_TTL_MS || 180_000);
const PEER_CENTER_CLEAN_INTERVAL = Math.max(30_000, Math.min(PEER_CENTER_TTL_MS / 2, 120_000));
let lastPeerCenterClean = 0;

export function getPeerCenterState(groupKey) {
  const k = String(groupKey || '');
  let s = peerCenterStateByGroup.get(k);
  if (!s) {
    s = {
      globalPeerMap: new Map(),
      digest: '0',
    };
    peerCenterStateByGroup.set(k, s);
  }
  const now = Date.now();
  if (now - lastPeerCenterClean > PEER_CENTER_CLEAN_INTERVAL) {
    cleanPeerCenterState(now);
  }
  s.lastTouch = Date.now();
  return s;
}

function cleanPeerCenterState(now = Date.now()) {
  lastPeerCenterClean = now;
  for (const [gk, s] of peerCenterStateByGroup.entries()) {
    for (const [pid, info] of s.globalPeerMap.entries()) {
      if (now - (info.lastSeen || 0) > PEER_CENTER_TTL_MS) {
        s.globalPeerMap.delete(pid);
      }
    }
    if (now - (s.lastTouch || 0) > PEER_CENTER_TTL_MS && s.globalPeerMap.size === 0) {
      peerCenterStateByGroup.delete(gk);
    }
  }
}

// 清理特定 peer 及其所有子设备信息
export function cleanPeerAndSubPeers(groupKey, peerId) {
  const state = getPeerCenterState(groupKey);
  const peerIdStr = String(peerId);
  
  // 清理该 peer 本身
  state.globalPeerMap.delete(peerIdStr);
  
  // 清理该 peer 的所有子设备（从其他 peer 的 directPeers 中移除）
  for (const [otherPeerId, peerInfo] of state.globalPeerMap.entries()) {
    if (peerInfo.directPeers && peerInfo.directPeers[peerIdStr]) {
      delete peerInfo.directPeers[peerIdStr];
      console.log(`[GlobalCleanup] Removed sub-peer ${peerIdStr} from peer ${otherPeerId}`);
    }
  }
  
  console.log(`[GlobalCleanup] Cleaned peer ${peerIdStr} and its sub-peers from group ${groupKey}`);
}

// 计算 peer 中心摘要
export function calcPeerCenterDigestFromMap(mapObj) {
  // 使用简单的同步哈希算法，避免异步问题
  let hash = 0n;
  const str = JSON.stringify(mapObj);
  
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5n) - hash) + BigInt(char);
    hash = hash & 0xFFFFFFFFFFFFFFFFn; // 限制为64位
  }
  
  return hash.toString();
}

// 构建 peer 中心响应映射
export function buildPeerCenterResponseMap(groupKey, state, peerManager) {
  const out = {};
  
  // 收集所有已知的 peer（包括直接连接的和通过路由信息发现的）
  const allKnownPeers = new Set();
  
  // 1. 添加直接连接的 peer
  const directPeers = peerManager.listPeerIdsInGroup(groupKey);
  directPeers.forEach(peerId => allKnownPeers.add(peerId));
  
  // 2. 添加通过路由信息发现的 peer
  const infos = peerManager._getPeerInfosMap(groupKey, false);
  if (infos) {
    for (const pid of infos.keys()) {
      allKnownPeers.add(pid);
    }
  }
  
  // 3. 添加全局 peer 映射中记录的 peer（包括子设备）
  for (const [peerId, peerInfo] of state.globalPeerMap.entries()) {
    allKnownPeers.add(Number(peerId));
    
    // 如果这个 peer 有直接连接的子设备，也添加到已知 peer 集合
    if (peerInfo.directPeers) {
      for (const subPeerId of Object.keys(peerInfo.directPeers)) {
        allKnownPeers.add(Number(subPeerId));
      }
    }
  }
  
  // 构建响应映射
  for (const peerId of allKnownPeers) {
    const key = String(peerId);
    const existing = state.globalPeerMap.get(key);
    out[key] = existing ? { ...existing } : { directPeers: {} };
    
    // 确保 directPeers 字段存在
    if (!out[key].directPeers) out[key].directPeers = {};
    
    // 如果这是直接连接的 peer，添加与两台服务器的连接信息
    if (directPeers.includes(peerId)) {
      out[key].directPeers[String(MY_PEER_ID)] = { latencyMs: 0 };
      out[key].directPeers[String(MY_PEER_ID_WS2)] = { latencyMs: 0 };
    }
    
    // 如果这个 peer 在全局映射中有子设备信息，保留这些信息
    if (existing && existing.directPeers) {
      for (const [subPeerId, subInfo] of Object.entries(existing.directPeers)) {
        out[key].directPeers[subPeerId] = { ...subInfo };
      }
    }
  }
  
  console.log(`[PeerCenter] Built response map for group ${groupKey} with ${allKnownPeers.size} peers`);
  return out;
}
