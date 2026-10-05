package indexer

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// IssueKind 是巡检发现问题的类别，逐类区分、不混成笼统失败。
type IssueKind int

const (
	// IssueMissing 索引链尖以内本该存在的高度缺失（真正的缺口）。
	IssueMissing IssueKind = iota
	// IssueMismatch 同一高度索引与数据源内容对不上（区块哈希/父哈希/交易集合）。
	IssueMismatch
	// IssueParentBroken 索引内部父链衔接断裂（父哈希与前一区块对不上）。
	IssueParentBroken
	// IssueExtraHeight 只存在于索引、高出数据源链尖的多余高度。
	IssueExtraHeight
	// IssueExtraTx 只存在于索引、不属于任何已提交区块的多余交易。
	IssueExtraTx
)

func (k IssueKind) String() string {
	switch k {
	case IssueMissing:
		return "missing-height"
	case IssueMismatch:
		return "content-mismatch"
	case IssueParentBroken:
		return "parent-broken"
	case IssueExtraHeight:
		return "extra-height"
	case IssueExtraTx:
		return "extra-tx"
	default:
		return "unknown"
	}
}

// Issue 是单条巡检发现，携带高度、区块标识与可解释的判定依据。
type Issue struct {
	Kind       IssueKind
	Height     uint64
	IndexHash  string // 索引侧区块哈希（无则空）
	SourceHash string // 数据源侧区块哈希（无则空）
	TxHash     string // 仅 IssueExtraTx 使用
	Detail     string
}

func (i Issue) String() string {
	switch i.Kind {
	case IssueExtraTx:
		return fmt.Sprintf("[%s] tx=%s: %s", i.Kind, i.TxHash, i.Detail)
	default:
		return fmt.Sprintf("[%s] height=%d index=%s source=%s: %s",
			i.Kind, i.Height, short(i.IndexHash), short(i.SourceHash), i.Detail)
	}
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// Report 是一次巡检对账的结论：按类别分开、只读、可解释。
type Report struct {
	SourceTip uint64
	IndexTip  uint64
	HasIndex  bool
	// Lag 是索引链尖落后数据源链尖的高度数；落后属于正常追赶，不计为问题。
	Lag     uint64
	Healthy bool
	Issues  []Issue
	// FirstMismatch 是最小高度上的链内不一致位置（缺失/内容不符/父链断裂），
	// 自愈时回滚到它之前；无链内不一致时为 0。
	FirstMismatch uint64
	// MaxIndexedHeight 为索引中出现过的最大高度（含多余高度）。
	MaxIndexedHeight uint64
}

// CountByKind 按类别统计问题数。
func (r *Report) CountByKind() map[IssueKind]int {
	m := make(map[IssueKind]int)
	for _, is := range r.Issues {
		m[is.Kind]++
	}
	return m
}

// IssuesOf 返回指定类别的全部问题。
func (r *Report) IssuesOf(k IssueKind) []Issue {
	var out []Issue
	for _, is := range r.Issues {
		if is.Kind == k {
			out = append(out, is)
		}
	}
	return out
}

// Summary 返回可解释的一句话结论与影响范围。
func (r *Report) Summary() string {
	if r.Healthy {
		return fmt.Sprintf("索引健康：索引链尖=%d 数据源链尖=%d 滞后=%d，未发现不一致",
			r.IndexTip, r.SourceTip, r.Lag)
	}
	c := r.CountByKind()
	parts := make([]string, 0, len(c))
	for _, k := range []IssueKind{IssueMissing, IssueMismatch, IssueParentBroken, IssueExtraHeight, IssueExtraTx} {
		if c[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s×%d", k, c[k]))
		}
	}
	return fmt.Sprintf("索引不一致：%s；首个不一致高度=%d，影响区间=[%d,%d]，数据源链尖=%d",
		strings.Join(parts, " "), r.FirstMismatch, r.FirstMismatch, r.MaxIndexedHeight, r.SourceTip)
}

// inspectSnapshot 在给定快照上执行纯函数式对账，不触碰任何持久化状态。
// source 提供数据源真值；快照只含已提交内容，因此不会读到摄入中间态。
func inspectSnapshot(snap Snapshot, sourceTip uint64, fetch func(uint64) (Block, error)) (*Report, error) {
	r := &Report{SourceTip: sourceTip, IndexTip: snap.TipHeight, HasIndex: snap.HasTip}

	var heights []uint64
	for h := range snap.Blocks {
		heights = append(heights, h)
		if h > r.MaxIndexedHeight {
			r.MaxIndexedHeight = h
		}
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	// 1) 多余高度：只存在于索引、高于数据源链尖。
	for _, h := range heights {
		if h > sourceTip {
			b := snap.Blocks[h]
			r.Issues = append(r.Issues, Issue{
				Kind: IssueExtraHeight, Height: h, IndexHash: b.Hash,
				Detail: fmt.Sprintf("数据源链尖=%d，该高度只存在于索引", sourceTip),
			})
		}
	}

	// 2) 逐高度核对 [1, min(索引链尖, 数据源链尖)]：缺失与内容不符。
	//    链尖之外尚未摄入属于正常追赶（Lag），不误判成脏数据。
	upper := snap.TipHeight
	if sourceTip < upper {
		upper = sourceTip
	}
	for h := uint64(1); h <= upper; h++ {
		b, ok := snap.Blocks[h]
		if !ok {
			r.Issues = append(r.Issues, Issue{
				Kind: IssueMissing, Height: h,
				Detail: fmt.Sprintf("索引链尖=%d 以内高度 %d 缺失（缺口）", snap.TipHeight, h),
			})
			r.markFirst(h)
			continue
		}
		srcB, err := fetch(h)
		if err != nil {
			return nil, err
		}
		if b.Hash != srcB.Hash {
			r.Issues = append(r.Issues, Issue{
				Kind: IssueMismatch, Height: h, IndexHash: b.Hash, SourceHash: srcB.Hash,
				Detail: "区块哈希与数据源不一致",
			})
			r.markFirst(h)
			continue
		}
		// 哈希一致时再核对交易集合，定位更细粒度的篡改。
		if diff := txSetDiff(b.Txs, srcB.Txs); diff != "" {
			r.Issues = append(r.Issues, Issue{
				Kind: IssueMismatch, Height: h, IndexHash: b.Hash, SourceHash: srcB.Hash,
				Detail: diff,
			})
			r.markFirst(h)
		}
	}

	// 3) 父链衔接：仅就索引内部相邻已提交块判定，断裂单独成类。
	for _, h := range heights {
		if h < 2 || h > upper {
			continue
		}
		b, ok := snap.Blocks[h]
		if !ok {
			continue
		}
		prev, prevOK := snap.Blocks[h-1]
		if !prevOK {
			r.Issues = append(r.Issues, Issue{
				Kind: IssueParentBroken, Height: h, IndexHash: b.Hash,
				Detail: fmt.Sprintf("前一高度 %d 缺失，父链无法衔接", h-1),
			})
			r.markFirst(h - 1)
			continue
		}
		if b.ParentHash != prev.Hash {
			r.Issues = append(r.Issues, Issue{
				Kind: IssueParentBroken, Height: h, IndexHash: b.Hash,
				Detail: fmt.Sprintf("parent=%s 与高度 %d 区块哈希 %s 不衔接",
					short(b.ParentHash), h-1, short(prev.Hash)),
			})
			r.markFirst(h)
		}
	}

	// 4) 多余交易：索引里存在但不属于任何已提交区块。
	owned := make(map[string]bool)
	for _, b := range snap.Blocks {
		for _, tx := range b.Txs {
			owned[tx.Hash] = true
		}
	}
	var extraTx []string
	for h := range snap.Txs {
		if !owned[h] {
			extraTx = append(extraTx, h)
		}
	}
	sort.Strings(extraTx)
	for _, h := range extraTx {
		r.Issues = append(r.Issues, Issue{
			Kind: IssueExtraTx, TxHash: h,
			Detail: "该交易不属于索引中任何已提交区块",
		})
	}

	if sourceTip > snap.TipHeight {
		r.Lag = sourceTip - snap.TipHeight
	}
	r.Healthy = len(r.Issues) == 0
	return r, nil
}

func (r *Report) markFirst(h uint64) {
	if r.FirstMismatch == 0 || h < r.FirstMismatch {
		r.FirstMismatch = h
	}
}

// txSetDiff 比较同高度两区块的交易集合，返回人可读差异，无差异返回空串。
func txSetDiff(a, b []Tx) string {
	ma := make(map[string]bool, len(a))
	for _, tx := range a {
		ma[tx.Hash] = true
	}
	mb := make(map[string]bool, len(b))
	for _, tx := range b {
		mb[tx.Hash] = true
	}
	var onlyIndex, onlySource []string
	for h := range ma {
		if !mb[h] {
			onlyIndex = append(onlyIndex, h)
		}
	}
	for h := range mb {
		if !ma[h] {
			onlySource = append(onlySource, h)
		}
	}
	if len(onlyIndex) == 0 && len(onlySource) == 0 {
		return ""
	}
	sort.Strings(onlyIndex)
	sort.Strings(onlySource)
	return fmt.Sprintf("交易集合不一致：索引多余=%v 数据源多余=%v", onlyIndex, onlySource)
}

// Inspect 拿当前索引与本地数据源做一次逐高度核对，全程只读：
// 先在单个读锁内取已提交快照，再与数据源比对，绝不改动任何持久化状态，
// 也不会与正在进行的摄入读到写一半的中间状态。
func (ix *Indexer) Inspect(ctx context.Context) (*Report, error) {
	snap := ix.store.Snapshot()
	r, err := inspectSnapshot(snap, ix.src.Tip(), func(h uint64) (Block, error) {
		return ix.fetchWithRetry(ctx, h)
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}
