package indexer

import (
	"context"
	"fmt"
)

// RepairResult 是一次自愈修复的结果说明。
type RepairResult struct {
	HealthyBefore bool   // 修复前巡检是否已健康（健康时零改动）
	Reverted      uint64 // 回滚区块数
	Rebuilt       uint64 // 从回滚点按序重建的区块数
	PurgedHeights uint64 // 清理的链尖之外多余高度数
	PurgedTxs     uint64 // 清理的多余（悬挂）交易数
	NewTip        uint64 // 修复后链尖高度
}

// Repair 拿当前索引与数据源对账，发现不一致时就地自愈：
//  1. 全程在 ix.mu 下执行，与正常摄入/其他修复互斥；只读快照来自单个读锁，
//     不会读到写一半的中间状态，也不会把正常追赶误判成脏数据；
//  2. 先清理高出当前链尖的多余高度，再回滚到第一个不一致位置之前，
//     从那里按顺序把受影响区间重建到数据源链尖；
//  3. 回滚深度受 MaxReorgDepth 约束，超限直接返回 KindReorgTooDeep 且
//     在任何改动之前拒绝，索引保持原样；
//  4. 被放弃的旧分支照常写旁路留痕；重复修复时首个区间已与数据源一致，
//     巡检判健康直接零改动，天然幂等。
func (ix *Indexer) Repair(ctx context.Context) (*Report, *RepairResult, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	snap := ix.store.Snapshot()
	report, err := inspectSnapshot(snap, ix.src.Tip(), func(h uint64) (Block, error) {
		return ix.fetchWithRetry(ctx, h)
	})
	if err != nil {
		return report, nil, err
	}
	res := &RepairResult{HealthyBefore: report.Healthy, NewTip: snap.TipHeight}
	if report.Healthy {
		// 索引干净：只读巡检结论，绝不改动任何持久化状态。
		return report, res, nil
	}

	// 第一步：清理高出当前链尖的多余高度（可能是人为污染或异常写入）。
	if report.MaxIndexedHeight > snap.TipHeight {
		n, err := ix.store.PurgeAbove(snap.TipHeight)
		if err != nil {
			return report, nil, err
		}
		res.PurgedHeights += uint64(n)
	}

	// 第二步：若存在链内不一致，做受深度上限约束的回滚重建。
	if report.FirstMismatch > 0 {
		base := report.FirstMismatch - 1
		tipH, _, _ := ix.store.Tip()
		depth := tipH - base // 与正常摄入重组同一口径
		if depth > ix.cfg.MaxReorgDepth {
			return report, nil, &Error{
				Kind: KindReorgTooDeep, Height: report.FirstMismatch,
				Reason: fmt.Sprintf("自愈回滚深度 %d 超过上限 %d（首个不一致高度 %d，回滚点 %d），索引保持原样",
					depth, ix.cfg.MaxReorgDepth, report.FirstMismatch, base),
			}
		}
		reverted, err := ix.abandonBranchLocked(base, ix.src.Tip())
		if err != nil {
			return report, nil, err
		}
		res.Reverted = uint64(reverted)
		// 按顺序重建 (base, sourceTip]，结论等价于清空后全量重放。
		target := ix.src.Tip()
		for h := base + 1; h <= target; h++ {
			nb, err := ix.fetchWithRetry(ctx, h)
			if err != nil {
				return report, nil, err
			}
			if err := ix.applyLocked(nb); err != nil {
				return report, nil, err
			}
			res.Rebuilt++
		}
		// 重建完成后的链尖即数据源链尖；对齐 next，避免后续重复扫描。
		ix.next = target + 1
	}

	// 第三步：数据源链尖之外仍可能残留多余高度（链内健康但索引更高时）。
	if tipH, _, _ := ix.store.Tip(); tipH > ix.src.Tip() {
		n, err := ix.store.PurgeAbove(ix.src.Tip())
		if err != nil {
			return report, nil, err
		}
		res.PurgedHeights += uint64(n)
		// 清理后索引稠密到数据源链尖，对齐 next，避免后续摄入跨过被删区间。
		ix.next = ix.src.Tip() + 1
	}

	// 第四步：清理不属于任何区块的多余交易。
	n, err := ix.store.PurgeDanglingTxs()
	if err != nil {
		return report, nil, err
	}
	res.PurgedTxs = uint64(n)

	if tipH, _, ok := ix.store.Tip(); ok {
		res.NewTip = tipH
	}
	return report, res, nil
}
