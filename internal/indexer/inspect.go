package indexer

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// 本文件实现巡检对账：逐高度比对索引与本地数据源，把问题按类别分开列出。
// 健康状态下巡检只读，不改动任何持久化状态。
//
// 巡检范围取“索引链尖”与“数据源链尖”的较小值，避免把摄入尚未到达的
// 正常缺口误判为脏数据；范围之外的高度单独按类别报出（多余/缺口）。

// IssueKind 是巡检问题的分类，类别之间互斥、可单独区分。
type IssueKind int

const (
	// IssueMissing 索引在已覆盖区间内缺失某个高度（高度缺口）。
	IssueMissing IssueKind = iota
	// IssueMismatch 同一高度索引区块与数据源内容对不上。
	IssueMismatch
	// IssueParentBroken 索引内父链衔接断裂（两个已存在高度之间）。
	IssueParentBroken
	// IssueExtraBlock 只存在于索引、超出数据源范围的多余高度。
	IssueExtraBlock
	// IssueOrphanTx 交易表中的记录不被其声称区块（或任何区块）引用。
	IssueOrphanTx
)

func (k IssueKind) String() string {
	switch k {
	case IssueMissing:
		return "missing"
	case IssueMismatch:
		return "mismatch"
	case IssueParentBroken:
		return "parent-broken"
	case IssueExtraBlock:
		return "extra-block"
	case IssueOrphanTx:
		return "orphan-tx"
	default:
		return "unknown"
	}
}

// Issue 是一条巡检发现，自带高度、区块标识与判定依据。
type Issue struct {
	Kind        IssueKind
	Height      uint64
	BlockHash   string // 索引侧区块哈希（若有）
	SourceHash  string // 数据源侧区块哈希（若有）
	TxHash      string // 仅 IssueOrphanTx 使用
	Description string // 可解释的判定依据
}

// InspectionReport 是巡检结论：健康时 Issues 为空。
type InspectionReport struct {
	Healthy     bool
	TipHeight   uint64 // 巡检快照时的索引链尖
	SourceTip   uint64
	CheckedFrom uint64
	CheckedTo   uint64
	Issues      []Issue
}

// byKind 返回指定类别的问题副本。
func (r *InspectionReport) byKind(k IssueKind) []Issue {
	out := make([]Issue, 0)
	for _, is := range r.Issues {
		if is.Kind == k {
			out = append(out, is)
		}
	}
	return out
}

// Missing 返回“已覆盖区间内缺失高度”类问题。
func (r *InspectionReport) Missing() []Issue { return r.byKind(IssueMissing) }

// Mismatches 返回“同高度内容对不上”类问题。
func (r *InspectionReport) Mismatches() []Issue { return r.byKind(IssueMismatch) }

// ParentBreaks 返回“父链衔接断裂”类问题。
func (r *InspectionReport) ParentBreaks() []Issue { return r.byKind(IssueParentBroken) }

// ExtraBlocks 返回“只存在于索引的多余高度”类问题。
func (r *InspectionReport) ExtraBlocks() []Issue { return r.byKind(IssueExtraBlock) }

// OrphanTxs 返回“多余/游离交易”类问题。
func (r *InspectionReport) OrphanTxs() []Issue { return r.byKind(IssueOrphanTx) }

// fetchReadOnly 拉取区块用于巡检：临时失败按次退避重试，但不写入任何
// 运行指标（巡检不影响摄入计数）；不可恢复失败立即返回。
func (ix *Indexer) fetchReadOnly(ctx context.Context, height uint64) (Block, error) {
	retries := ix.cfg.MaxRetries
	if retries < 1 {
		retries = maxFetchRetries
	}
	for attempt := 0; ; attempt++ {
		b, err := ix.src.FetchBlock(height)
		if err == nil {
			return b, nil
		}
		switch KindOf(err) {
		case KindTransientSource:
			if attempt >= retries {
				return Block{}, err
			}
			select {
			case <-ctx.Done():
				return Block{}, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		default:
			return Block{}, err
		}
	}
}

// inspectHeight 对单个高度做对账（不修改任何状态）。
func inspectHeight(snap storeSnapshot, h uint64, srcB Block, srcOK bool, issues []Issue) []Issue {
	stored, indexOK := snap.blocks[h]
	switch {
	case indexOK && srcOK:
		if stored.Hash != srcB.Hash || !blockEqual(stored, srcB) {
			issues = append(issues, Issue{
				Kind: IssueMismatch, Height: h,
				BlockHash:  stored.Hash,
				SourceHash: srcB.Hash,
				Description: fmt.Sprintf(
					"高度 %d 内容对不上: 索引块=%s 数据源块=%s", h, stored.Hash, srcB.Hash),
			})
			return issues
		}
		// 哈希与内容一致，再核对该高度交易表是否都正确指向本块。
		for _, tx := range stored.Txs {
			it, ok := snap.txs[tx.Hash]
			if !ok {
				issues = append(issues, Issue{
					Kind: IssueOrphanTx, Height: h, TxHash: tx.Hash,
					BlockHash: stored.Hash,
					Description: fmt.Sprintf(
						"交易 %s 属于高度 %d 区块 %s，但交易表缺失", tx.Hash, h, stored.Hash),
				})
				continue
			}
			if it.BlockHeight != h || it.BlockHash != stored.Hash {
				issues = append(issues, Issue{
					Kind: IssueOrphanTx, Height: it.BlockHeight, TxHash: tx.Hash,
					BlockHash: it.BlockHash,
					Description: fmt.Sprintf(
						"交易 %s 索引指向 (%d,%s)，但其所在区块 %s 位于高度 %d",
						tx.Hash, it.BlockHeight, it.BlockHash, stored.Hash, h),
				})
			}
		}
	case !indexOK && srcOK:
		issues = append(issues, Issue{
			Kind: IssueMissing, Height: h, SourceHash: srcB.Hash,
			Description: fmt.Sprintf(
				"高度 %d 缺失: 数据源块=%s，索引在已覆盖区间内无此高度", h, srcB.Hash),
		})
	case indexOK && !srcOK:
		issues = append(issues, Issue{
			Kind: IssueExtraBlock, Height: h, BlockHash: stored.Hash,
			Description: fmt.Sprintf(
				"高度 %d 只存在于索引: 块=%s，数据源已无此高度", h, stored.Hash),
		})
	}
	return issues
}

// blockEqual 逐字段比较两个区块内容（哈希一致时仍兜底校验内容）。
func blockEqual(a, b Block) bool {
	if a.Height != b.Height || a.Hash != b.Hash || a.ParentHash != b.ParentHash {
		return false
	}
	if len(a.Txs) != len(b.Txs) {
		return false
	}
	for i := range a.Txs {
		if a.Txs[i] != b.Txs[i] {
			return false
		}
	}
	return true
}

// Inspect 对索引与数据源做只读逐高度对账，绝不修改持久化状态。
// 巡检开始时在 stateMu 读锁内取一致性快照：若与重组/自愈相撞，只会
// 看到完整的回滚前或重建后状态，不会读到写到一半的中间状态。
func (ix *Indexer) Inspect(ctx context.Context) (*InspectionReport, error) {
	ix.stateMu.RLock()
	defer ix.stateMu.RUnlock()
	return ix.inspectUnderLock(ctx)
}

// inspectUnderLock 是巡检核心：调用方须已持有 stateMu（读或写），
// 本函数不再重复加锁，只在持锁状态下取一致性快照。
func (ix *Indexer) inspectUnderLock(ctx context.Context) (*InspectionReport, error) {
	snap := ix.store.Snapshot()

	sourceTip := ix.src.Tip()
	rep := &InspectionReport{
		TipHeight: snap.tipHeight,
		SourceTip: sourceTip,
	}
	if !snap.hasTip {
		rep.CheckedFrom, rep.CheckedTo = 1, 0
		rep.Healthy = true
		return rep, nil
	}

	issues := make([]Issue, 0)

	// 逐高度对账区间 [1, min(indexTip, sourceTip)]。
	upper := snap.tipHeight
	if sourceTip < upper {
		upper = sourceTip
	}
	rep.CheckedFrom, rep.CheckedTo = 1, upper
	for h := uint64(1); h <= upper; h++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		srcB, err := ix.fetchReadOnly(ctx, h)
		if err != nil {
			return nil, err
		}
		issues = inspectHeight(snap, h, srcB, true, issues)
	}

	// 索引超出数据源范围的高度：多余数据。
	for h := sourceTip + 1; h <= snap.tipHeight && sourceTip > 0; h++ {
		if b, ok := snap.blocks[h]; ok {
			issues = append(issues, Issue{
				Kind: IssueExtraBlock, Height: h, BlockHash: b.Hash,
				Description: fmt.Sprintf(
					"高度 %d 超出数据源链尖 %d，块=%s 只存在于索引", h, sourceTip, b.Hash),
			})
		}
	}

	// 索引内父链衔接：仅对“相邻且都存在”的高度判定断裂。
	heights := make([]uint64, 0, len(snap.blocks))
	for h := range snap.blocks {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	for i := 1; i < len(heights); i++ {
		curH, prevH := heights[i], heights[i-1]
		if curH != prevH+1 {
			continue // 中间有缺高，已归入 missing，不重复报父链断裂
		}
		cur, prev := snap.blocks[curH], snap.blocks[prevH]
		if cur.ParentHash != prev.Hash {
			issues = append(issues, Issue{
				Kind: IssueParentBroken, Height: curH,
				BlockHash:  cur.Hash,
				SourceHash: cur.ParentHash,
				Description: fmt.Sprintf(
					"高度 %d 父链断裂: 块=%s 的 parent=%s，与高度 %d 块=%s 不衔接",
					curH, cur.Hash, cur.ParentHash, prevH, prev.Hash),
			})
		}
	}

	// 游离交易：交易表中存在、但不被任何已索引区块包含的记录。
	referenced := make(map[string]bool, len(snap.txs))
	for _, b := range snap.blocks {
		for _, tx := range b.Txs {
			referenced[tx.Hash] = true
		}
	}
	txHashes := make([]string, 0, len(snap.txs))
	for txh := range snap.txs {
		txHashes = append(txHashes, txh)
	}
	sort.Strings(txHashes)
	for _, txh := range txHashes {
		if referenced[txh] {
			continue
		}
		it := snap.txs[txh]
		issues = append(issues, Issue{
			Kind: IssueOrphanTx, Height: it.BlockHeight, TxHash: txh,
			BlockHash: it.BlockHash,
			Description: fmt.Sprintf(
				"交易 %s 只存在于索引交易表，声称属于 (%d,%s)，但无区块引用",
				txh, it.BlockHeight, it.BlockHash),
		})
	}

	sortIssues(issues)
	rep.Issues = issues
	rep.Healthy = len(issues) == 0
	return rep, nil
}

// sortIssues 按高度再按类别排序，保证输出稳定可解释。
func sortIssues(issues []Issue) {
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Height != issues[j].Height {
			return issues[i].Height < issues[j].Height
		}
		if issues[i].Kind != issues[j].Kind {
			return issues[i].Kind < issues[j].Kind
		}
		return issues[i].TxHash < issues[j].TxHash
	})
}
