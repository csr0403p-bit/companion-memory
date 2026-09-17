// louvain.js —— 纯 JS Louvain 社区发现（无依赖，从云端 src/louvain.ts 移植 2026-08-22）
// 2026-08-07 云端版: archive 事件网络 rebuild 后自动聚类用。
// 完整版: 节点移动阶段 + 粗化阶段（社区→超节点迭代），模块度单调提升。
// 复杂度: 每轮 O(m)，通常 3-8 轮收敛。99 节点/400 边 <100ms。
'use strict';

// 输入: nodes = number[] (节点 id), edges = [number, number][] (无向边)
// 输出: Map<nodeId, communityId> —— 社区 id 0..k-1 连续
function louvain(nodes, edges, randomSeed = 42) {
  let seed = randomSeed;
  const rnd = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
  const shuffled = (arr) => {
    const a = [...arr];
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(rnd() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  };

  function buildAdj(nodesArr, edgesArr) {
    const adj = new Map();
    for (const n of nodesArr) adj.set(n, new Map());
    for (const [a, b] of edgesArr) {
      if (a === b) continue;
      adj.get(a).set(b, (adj.get(a).get(b) ?? 0) + 1);
      adj.get(b).set(a, (adj.get(b).get(a) ?? 0) + 1);
    }
    return adj;
  }

  // m = 总边权重: 非自环每条 1 次, 自环 1 次 (Louvain 标准定义)
  function totalEdgeWeight(adj) {
    let m = 0;
    for (const [a, nb] of adj) {
      for (const [b, w] of nb) {
        if (a !== b) m += w;
        else m += w * 2;  // 自环在 adj 里存 1 次, 标准定义计 2(与无向边一致)
      }
    }
    return m / 2;
  }

  // 节点度数: 自环计 2 次(与 networkx/python-louvain 一致)
  function nodeDegree(adj, n) {
    let d = 0;
    for (const [j, w] of adj.get(n)) {
      d += w;
      if (j === n) d += w;
    }
    return d;
  }

  function computeQ(adj, partition) {
    const m = totalEdgeWeight(adj);
    if (m === 0) return 0;
    const tot = new Map();
    for (const n of adj.keys()) {
      const d = nodeDegree(adj, n);
      const c = partition.get(n);
      tot.set(c, (tot.get(c) ?? 0) + d);
    }
    // Σin: 内部边权重去重后每条一次(自环一次) —— 对齐 python-louvain modularity
    let q = 0;
    for (const [n, nb] of adj) {
      for (const [b, w] of nb) {
        if (partition.get(n) !== partition.get(b)) continue;
        q += (n === b ? w : w / 2);  // 非自环双向遍历 → /2 补回一次; 自环本就一次
      }
    }
    q /= m;
    for (const t of tot.values()) q -= Math.pow(t / (2 * m), 2);
    return q;
  }

  // 节点移动阶段: 每个节点尝试移入使模块度增益最大的邻居社区
  function movePass(adj, nodesArr, shuffled) {
    const nodeComm = new Map();
    const commNodes = new Map();
    const commTot = new Map();
    const degree = new Map();
    let curId = 0;
    for (const n of nodesArr) {
      nodeComm.set(n, curId);
      commNodes.set(curId, [n]);
      curId++;
    }
    for (const n of nodesArr) degree.set(n, nodeDegree(adj, n));
    for (const [cid, ns] of commNodes) {
      let t = 0;
      for (const n of ns) t += degree.get(n);
      commTot.set(cid, t);
    }
    const m = totalEdgeWeight(adj);

    for (let pass = 0; pass < 50; pass++) {
      let moved = false;
      for (const i of shuffled(nodesArr)) {
        const cur = nodeComm.get(i);
        const k_i = degree.get(i);
        const links = new Map();
        for (const [j, w] of adj.get(i)) {
          if (j === i) continue;  // 自环不参与增益计算(对齐 python-louvain __neighcom)
          const cj = nodeComm.get(j);
          links.set(cj, (links.get(cj) ?? 0) + w);
        }
        // 完整移动增益 = 移除代价 + 插入增益 (对齐 python-louvain)
        const wInCur = links.get(cur) ?? 0;
        const totCur = commTot.get(cur);
        const removeCost = -wInCur / (2 * m) + ((totCur - k_i) * k_i) / (2 * m * m);
        let best = cur;
        let bestGain = 0;
        for (const [cid, wIn] of links) {
          if (cid === cur) continue;
          const tot = commTot.get(cid) ?? 0;
          if (tot === 0) continue;  // 空社区不作候选(防振荡)
          // ΔQ = removeCost + k_i,in(C)/(2m) - (Σtot(C)·k_i)/(2m²)
          const incr = removeCost + wIn / (2 * m) - (tot * k_i) / (2 * m * m);
          if (incr > bestGain) { bestGain = incr; best = cid; }
        }
        if (best !== cur) {
          const arr = commNodes.get(cur);
          const idx = arr.indexOf(i);
          if (idx >= 0) arr.splice(idx, 1);
          commNodes.get(best).push(i);
          nodeComm.set(i, best);
          commTot.set(cur, commTot.get(cur) - k_i);
          commTot.set(best, commTot.get(best) + k_i);
          moved = true;
        }
      }
      if (!moved) break;
    }
    return nodeComm;
  }

  // 粗化: 社区 → 超节点, 社区间边权重合并(含自环)
  function coarsen(adj, partition, commNodes) {
    const commToNode = new Map();
    const newAdj = new Map();
    let next = 0;
    for (const cid of commNodes.keys()) {
      commToNode.set(cid, next);
      newAdj.set(next, new Map());
      next++;
    }
    for (const [a, nb] of adj) {
      const ca = commToNode.get(partition.get(a));
      for (const [b, w] of nb) {
        const cb = commToNode.get(partition.get(b));
        newAdj.get(ca).set(cb, (newAdj.get(ca).get(cb) ?? 0) + w);
      }
    }
    return newAdj;
  }

  let adj = buildAdj(nodes, edges);
  const origAdj = adj;  // 原始图: Q 始终在原始图上算(层级间可比)
  const isolated = nodes.filter(n => (adj.get(n)?.size ?? 0) === 0);
  const active = nodes.filter(n => (adj.get(n)?.size ?? 0) > 0);
  if (active.length === 0) {
    const out = new Map();
    isolated.forEach((n, i) => out.set(n, i));
    return out;
  }

  // levelMap: 当前层节点 → 原始节点集合（粗化后追溯）
  let levelMap = new Map();
  for (const n of active) levelMap.set(n, [n]);
  let curNodes = active;

  let bestPartition = null;
  let bestLevelMap = levelMap;
  let bestQ = -Infinity;

  for (let iter = 0; iter < 25; iter++) {
    const partition = movePass(adj, curNodes, shuffled);
    // Q 展开到原始节点后在原始图上计算 —— 粗化层级间严格可比
    const origPartition = new Map();
    {
      const cmap = new Map();
      let cid2n = 0;
      for (const [layerNode, c] of partition) {
        const origs = levelMap.get(layerNode) ?? [layerNode];
        if (!cmap.has(c)) cmap.set(c, cid2n++);
        for (const o of origs) origPartition.set(o, cmap.get(c));
      }
    }
    const Q = computeQ(origAdj, origPartition);
    if (Q <= bestQ + 1e-12) break;  // 模块度不再提升
    bestQ = Q;
    bestPartition = partition;
    bestLevelMap = levelMap;

    // 粗化准备
    const commNodes = new Map();
    for (const [n, c] of partition) {
      if (!commNodes.has(c)) commNodes.set(c, []);
      commNodes.get(c).push(n);
    }
    if (commNodes.size >= curNodes.length) break;  // 没有合并
    // coarsen 按 commNodes.keys() 顺序编号 0..k-1 —— 与主循环的新节点 id 对齐
    const cidList = [...commNodes.keys()];
    const newIds = cidList.map((_, i) => i);
    const nextLevelMap = new Map();
    newIds.forEach((newId, i) => {
      const flat = [];
      for (const n of commNodes.get(cidList[i])) flat.push(...(levelMap.get(n) ?? []));
      nextLevelMap.set(newId, flat);
    });
    adj = coarsen(adj, partition, commNodes);
    curNodes = newIds;
    levelMap = nextLevelMap;
  }

  // 展开: 原始节点 → 社区
  const result = new Map();
  if (bestPartition) {
    const idMap = new Map();
    let next = 0;
    for (const [layerNode, c] of bestPartition) {
      const origs = bestLevelMap.get(layerNode) ?? [layerNode];
      if (!idMap.has(c)) idMap.set(c, next++);
      for (const o of origs) result.set(o, idMap.get(c));
    }
  }
  isolated.forEach((n, i) => result.set(n, (bestPartition ? new Set(bestPartition.values()).size : 0) + i));
  return result;
}

module.exports = { louvain };
