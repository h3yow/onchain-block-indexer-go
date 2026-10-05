package indexer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Config 是流水线配置。
type Config struct {
	StoreDir      string      // 检查点持久化目录
	MaxReorgDepth uint64      // 允许的最大重组深度，超过即拒绝
	Workers       int         // 并发拉取协程数（<1 时按 1 处理）
	PollBlocks    uint64      // 本次同步的目标高度上限（0 表示跟到数据源链尖）
	MaxRetries    int         // 临时失败的最大重试次数（<1 时按 5 处理）
	Audit         AuditConfig // 重组留痕（旁路审计）配置
}

// Metrics 是关键运行指标（原子计数，可并发读取）。
type Metrics struct {
	BlocksApplied  atomic.Uint64
	TxsApplied     atomic.Uint64
	Duplicates     atomic.Uint64
	Reorgs         atomic.Uint64
	BlocksReverted atomic.Uint64
	TransientErrs  atomic.Uint64
	FatalErrs      atomic.Uint64
	InvalidBlocks  atomic.Uint64
	// AuditRecords 为写入留痕的区块条数；AuditFailures 为留痕写入失败次数
	// （留痕是旁路，失败不影响索引正确性，只计数暴露）。
	AuditRecords  atomic.Uint64
	AuditFailures atomic.Uint64
}

// Indexer 是索引流水线：并发拉取、按序提交、重组回滚。
// 提交与重组由 ix.mu 序列化，查询只读已提交状态，无中间破损。
type Indexer struct {
	cfg   Config
	src   Source
	store *Store
	audit *AuditLog
	Met   Metrics

	mu   sync.Mutex // 序列化提交与重组；巡检后的自愈与修复也走这把锁
	next uint64     // 下一个待摄入高度
}

// NewIndexer 创建流水线并从持久化进度恢复（不重复不丢失）。
func NewIndexer(cfg Config, src Source) (*Indexer, error) {
	st, err := OpenStore(cfg.StoreDir)
	if err != nil {
		return nil, err
	}
	al, err := OpenAuditLog(cfg.StoreDir, cfg.Audit)
	if err != nil {
		return nil, err
	}
	ix := &Indexer{cfg: cfg, src: src, store: st, audit: al, next: 1}
	if h, _, ok := st.Tip(); ok {
		ix.next = h + 1
	}
	return ix, nil
}

// Audit 返回重组留痕日志（未启用时为 nil，调用其方法是安全的空操作）。
func (ix *Indexer) Audit() *AuditLog { return ix.audit }

// Store 返回底层存储的查询入口。
func (ix *Indexer) Store() *Store { return ix.store }

// Next 返回下一个待摄入高度。
func (ix *Indexer) Next() uint64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.next
}

const maxFetchRetries = 5

// fetchWithRetry 拉取区块：临时失败按次重试并计数，不可恢复失败立即返回。
func (ix *Indexer) fetchWithRetry(ctx context.Context, height uint64) (Block, error) {
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
			ix.Met.TransientErrs.Add(1)
			if attempt >= retries {
				return Block{}, err
			}
			select {
			case <-ctx.Done():
				return Block{}, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		case KindFatalSource:
			ix.Met.FatalErrs.Add(1)
			return Block{}, err
		default:
			return Block{}, err
		}
	}
}

// findCommonAncestor 从 fromHeight 向下寻找索引与数据源一致的共同祖先高度；
// 返回 0 表示共同祖先在创世之前（需全量回滚）。
func (ix *Indexer) findCommonAncestor(ctx context.Context, fromHeight uint64) (uint64, error) {
	tipH, _, _ := ix.store.Tip()
	h := fromHeight
	if tipH < h {
		h = tipH
	}
	for h >= 1 {
		stored, ok := ix.store.BlockAt(h)
		srcB, err := ix.fetchWithRetry(ctx, h)
		if err != nil {
			return 0, err
		}
		if ok && stored.Hash == srcB.Hash {
			return h, nil
		}
		h--
	}
	return 0, nil
}

// applyLocked 在持锁状态下直接应用一个区块并累计指标。
func (ix *Indexer) applyLocked(b Block) error {
	applied, err := ix.store.ApplyBlock(b)
	if err != nil {
		if KindOf(err) == KindInvalidData {
			ix.Met.InvalidBlocks.Add(1)
		}
		return err
	}
	if applied {
		ix.Met.BlocksApplied.Add(1)
		ix.Met.TxsApplied.Add(uint64(len(b.Txs)))
	} else {
		ix.Met.Duplicates.Add(1)
	}
	return nil
}

// abandonBranchLocked 在持 ix.mu 状态下回滚到共同祖先 base，并把被放弃
// 的旧分支区块（高度 (base, tip] 中存在的）记入旁路留痕。newTip 为重组
// 完成后的新链尖高度（提交场景为当前块高度，修复场景为重建目标）。
// 留痕写入失败不回滚索引、不阻断主流程，仅累计 AuditFailures。
func (ix *Indexer) abandonBranchLocked(base, newTip uint64) (int, error) {
	tipH, _, _ := ix.store.Tip()
	var abandoned []Block
	for h := tipH; h > base; h-- {
		if b, ok := ix.store.BlockAt(h); ok {
			abandoned = append(abandoned, b)
		}
	}
	reverted, err := ix.store.RollbackTo(base)
	if err != nil {
		return 0, err
	}
	ix.Met.Reorgs.Add(1)
	ix.Met.BlocksReverted.Add(uint64(reverted))
	if len(abandoned) > 0 {
		n, aerr := ix.audit.Append(abandoned, base, newTip)
		if aerr != nil {
			ix.Met.AuditFailures.Add(1)
		} else {
			ix.Met.AuditRecords.Add(uint64(n))
		}
	}
	return reverted, nil
}

// commit 在 ix.mu 下提交一个区块，必要时执行重组：回滚到共同祖先并
// 同步重建受影响区间，保证 Run 的按序提交窗口不受影响。
func (ix *Indexer) commit(ctx context.Context, b Block) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	tipH, tipHash, hasTip := ix.store.Tip()

	// 已摄入区间且哈希一致：幂等重复，直接跳过。
	if hasTip && b.Height <= tipH {
		if existing, ok := ix.store.BlockAt(b.Height); ok && existing.Hash == b.Hash {
			ix.Met.Duplicates.Add(1)
			if ix.next == b.Height {
				ix.next = b.Height + 1
			}
			return nil
		}
	}

	conflict := hasTip && (b.Height <= tipH ||
		(b.Height == tipH+1 && b.ParentHash != tipHash))
	if conflict {
		ancestor, err := ix.findCommonAncestor(ctx, b.Height-1)
		if err != nil {
			return err
		}
		depth := tipH - ancestor
		if depth > ix.cfg.MaxReorgDepth {
			return &Error{
				Kind: KindReorgTooDeep, Height: b.Height, BlockHash: b.Hash,
				Reason: fmt.Sprintf("重组深度 %d 超过上限 %d，共同祖先高度 %d",
					depth, ix.cfg.MaxReorgDepth, ancestor),
			}
		}
		if _, err := ix.abandonBranchLocked(ancestor, b.Height); err != nil {
			return err
		}
		// 同步重建 (ancestor, b.Height) 区间。
		for h := ancestor + 1; h < b.Height; h++ {
			nb, err := ix.fetchWithRetry(ctx, h)
			if err != nil {
				return err
			}
			if err := ix.applyLocked(nb); err != nil {
				return err
			}
		}
		if b.Height <= ancestor {
			// 该区块属于被抛弃分支，其高度已由新分支重建。
			ix.next = b.Height + 1
			return nil
		}
		// 回滚后父哈希仍不符，说明 b 是分叉前的旧数据，重新拉取。
		if _, newTip, _ := ix.store.Tip(); b.ParentHash != newTip {
			if b, err = ix.fetchWithRetry(ctx, b.Height); err != nil {
				return err
			}
		}
	}

	if err := ix.applyLocked(b); err != nil {
		return err
	}
	if ix.next <= b.Height {
		ix.next = b.Height + 1
	}
	return nil
}

// Run 并发拉取、按高度顺序提交，直到目标高度或出现不可恢复错误。
// 临时失败自动重试；数据非法、超限重组、不可恢复数据源错误会终止并返回。
func (ix *Indexer) Run(ctx context.Context) error {
	target := ix.cfg.PollBlocks
	if tip := ix.src.Tip(); target == 0 || target > tip {
		target = tip
	}
	workers := ix.cfg.Workers
	if workers < 1 {
		workers = 1
	}

	type fetchResult struct {
		height uint64
		b      Block
		err    error
	}
	resCh := make(chan fetchResult, workers)
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	start := ix.Next()
	if start > target {
		return nil
	}
	// 生产者：并发拉取 [start, target]。
	go func() {
		defer close(resCh)
		for h := start; h <= target; h++ {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(h uint64) {
				defer wg.Done()
				defer func() { <-sem }()
				b, err := ix.fetchWithRetry(ctx, h)
				resCh <- fetchResult{height: h, b: b, err: err}
			}(h)
		}
		wg.Wait()
	}()

	pending := make(map[uint64]fetchResult)
	for ix.Next() <= target {
		r, ok := pending[ix.Next()]
		if !ok {
			var more bool
			r, more = <-resCh
			if !more {
				return nil // 源已关闭（ctx 取消）
			}
			if r.height != ix.Next() {
				pending[r.height] = r
				continue
			}
		}
		delete(pending, r.height)
		if r.err != nil {
			return r.err
		}
		if err := ix.commit(ctx, r.b); err != nil {
			return err
		}
	}
	return nil
}
