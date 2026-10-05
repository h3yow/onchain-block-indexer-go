package indexer

import (
	"context"
	"fmt"
)

// 本文件实现巡检自愈：回滚到第一个对不上的位置，再按顺序重建受影响区间。
// 修复受现有重组深度上限（MaxReorgDepth）约束；超限拒绝并保持索引原样。
//
// 整个“对账-回滚-重建”在 stateMu 写锁与 ix.mu 下原子进行：与正在进行的
// 摄入互斥，巡检也只能看到修复前或修复后的完整状态。

// HealReport 描述一次自愈的结论与影响范围。
type HealReport struct {
	Healed         bool   // 是否执行了修复（健康时为 false，且不改动任何状态）
	AncestorHeight uint64 // 回滚落点（共同祖先，0 表示创世之前）
	FirstBadHeight uint64 // 第一个对不上的高度
	Reverted       int    // 回滚的区块数
	Rebuilt        int    // 重建的区块数
	CheckedFrom    uint64
	CheckedTo      uint64
	IssuesBefore   []Issue // 修复前发现的问题快照
}

// findHealAncestor 从 startHeight 向下寻找索引与数据源一致的共同祖先；
// 返回 0 表示共同祖先在创世之前。
func (ix *Indexer) findHealAncestor(ctx context.Context, startHeight uint64) (uint64, error) {
	for h := int64(startHeight); h >= 1; h-- {
		hh := uint64(h)
		stored, ok := ix.store.BlockAt(hh)
		if !ok {
			continue // 缺高处继续向下
		}
		srcB, err := ix.fetchWithRetry(ctx, hh)
		if err != nil {
			return 0, err
		}
		if stored.Hash == srcB.Hash {
			return hh, nil
		}
	}
	return 0, nil
}

// Heal 巡检发现不一致时就地自愈；健康时为空操作，不改动任何状态。
// 修复结果与“清空后全量重放”一致；重复执行幂等。
func (ix *Indexer) Heal(ctx context.Context) (*HealReport, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.stateMu.Lock()
	defer ix.stateMu.Unlock()

	rep, err := ix.inspectUnderLock(ctx)
	if err != nil {
		return nil, err
	}
	hr := &HealReport{
		CheckedFrom:  rep.CheckedFrom,
		CheckedTo:    rep.CheckedTo,
		IssuesBefore: append([]Issue(nil), rep.Issues...),
	}
	if rep.Healthy {
		// 索引干净：只读巡检，绝不改动任何持久化状态。
		return hr, nil
	}

	// 第一个对不上的高度 = 报告中最小高度。
	firstBad := rep.Issues[0].Height
	for _, is := range rep.Issues[1:] {
		if is.Height < firstBad {
			firstBad = is.Height
		}
	}
	hr.FirstBadHeight = firstBad

	// 从第一个问题高度向上找最高的正确高度作为搜索起点，再向下找共同祖先。
	searchStart := uint64(0)
	if firstBad >= 1 {
		searchStart = firstBad - 1
	}
	tipH, _, _ := ix.store.Tip()
	if searchStart > tipH {
		searchStart = tipH
	}
	ancestor, err := ix.findHealAncestor(ctx, searchStart)
	if err != nil {
		return nil, err
	}
	hr.AncestorHeight = ancestor

	depth := tipH - ancestor
	if depth > ix.cfg.MaxReorgDepth {
		return hr, &Error{
			Kind: KindReorgTooDeep, Height: firstBad,
			Reason: fmt.Sprintf(
				"自愈需要回滚深度 %d 超过上限 %d（共同祖先高度 %d，链尖 %d），已拒绝并保持索引原样",
				depth, ix.cfg.MaxReorgDepth, ancestor, tipH),
		}
	}

	// 回滚到共同祖先，并把被放弃分支写入旁路留痕（标记为 heal）。
	reverted, err := ix.store.rollbackWithReason(ancestor, AbandonByHeal)
	if err != nil {
		return nil, err
	}
	hr.Reverted = reverted

	// 按顺序重建 (ancestor, sourceTip]，使结论与全量重放一致。
	target := ix.src.Tip()
	if ix.cfg.PollBlocks > 0 && ix.cfg.PollBlocks < target {
		target = ix.cfg.PollBlocks
	}
	for h := ancestor + 1; h <= target; h++ {
		nb, err := ix.fetchWithRetry(ctx, h)
		if err != nil {
			return nil, err
		}
		if err := ix.applyLocked(nb); err != nil {
			return nil, err
		}
		hr.Rebuilt++
	}
	if ix.next <= target {
		ix.next = target + 1
	}
	hr.Healed = true
	return hr, nil
}
