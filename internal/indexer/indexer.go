package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/onchain-block-indexer-go/internal/chain"
	"github.com/h3yow/onchain-block-indexer-go/internal/store"
)

// 默认配置。
const (
	defaultMaxReorgDepth = 10
	defaultMaxRetries    = 3
)

// Config 为索引器可配置项。
type Config struct {
	// MaxReorgDepth 是允许回滚抛弃的最大区块数；超过则明确拒绝重组。
	// 0 表示采用默认值 10。
	MaxReorgDepth int64
	// MaxRetries 是数据源临时失败时 Ingest 的重试上限（CatchUp 内部使用）。
	// 0 表示采用默认值 3。
	MaxRetries int
	// RetryBackoff 是临时失败重试之间的退避时长；0 表示 10ms。
	RetryBackoff time.Duration
	// Logf 用于输出带高度、区块标识与判定依据的可解释日志；nil 表示静默。
	Logf func(format string, args ...any)
}

func (c *Config) withDefaults() {
	if c.MaxReorgDepth <= 0 {
		c.MaxReorgDepth = defaultMaxReorgDepth
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 10 * time.Millisecond
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}

// ReorgTooDeepError 表示重组深度超过配置上限；索引状态保持拒绝前原样。
type ReorgTooDeepError struct {
	Depth          int64
	MaxDepth       int64
	OldTipHash     string
	OldTipHeight   int64
	NewTipHash     string
	AncestorHeight int64 // -1 表示连创世块都不一致
}

func (e *ReorgTooDeepError) Error() string {
	return fmt.Sprintf("reorg rejected: rollback depth %d exceeds max %d (old tip=%s@%d, new tip=%s, ancestor height=%d)",
		e.Depth, e.MaxDepth, e.OldTipHash, e.OldTipHeight, e.NewTipHash, e.AncestorHeight)
}

// Indexer 串行化区块提交，保证并发摄入下索引状态一致。
type Indexer struct {
	cfg     Config
	source  chain.DataSource
	store   store.Store
	metrics Metrics

	commitMu chan struct{} // 以单元素信号量实现的提交互斥锁
}

// New 创建索引器。
func New(cfg Config, src chain.DataSource, st store.Store) *Indexer {
	cfg.withDefaults()
	return &Indexer{
		cfg:      cfg,
		source:   src,
		store:    st,
		commitMu: make(chan struct{}, 1),
	}
}

// lock 获取提交锁（可被 ctx 取消）。
func (ix *Indexer) lock(ctx context.Context) error {
	select {
	case ix.commitMu <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ix *Indexer) unlock() { <-ix.commitMu }

// Ingest 向链尖推进一步：
//   - 已与数据源链尖一致：返回 (false, nil)，重复摄入幂等；
//   - 下一区块顺接当前链头：原子写入，返回 (true, nil)；
//   - 下一区块不衔接：按共同祖先回滚并重建成链上的第一个区块；
//   - 数据源临时失败：返回归类为 temporary 的错误，进度不变，可重试；
//   - 字段非法/不可恢复：返回 fatal 错误，进度不变。
func (ix *Indexer) Ingest(ctx context.Context) (bool, error) {
	if err := ix.lock(ctx); err != nil {
		return false, err
	}
	defer ix.unlock()

	tip, err := ix.source.Tip(ctx)
	if err != nil {
		return false, ix.classify("fetch tip", -1, err)
	}

	head, err := ix.store.Head()
	if err != nil && err != store.ErrNotFound {
		return false, fmt.Errorf("indexer: read head: %w", err)
	}
	caughtUp := err == nil

	if !caughtUp {
		// 冷启动：从创世块开始。
		return ix.ingestGenesis(ctx, tip)
	}

	if head.Hash == tip.Hash {
		// 幂等：已在链尖，重复摄入不产生任何记录。
		ix.metrics.DuplicateIngest.Add(1)
		ix.cfg.Logf("ingest skip: already at tip height=%d block=%s basis=local head equals source tip (idempotent)",
			head.Height, head.Hash)
		return false, nil
	}

	next := head.Height + 1
	blk, err := ix.source.BlockAt(ctx, next, tip.Hash)
	if err != nil {
		return false, ix.classify(fmt.Sprintf("fetch block height=%d", next), next, err)
	}

	if blk.ParentHash == head.Hash {
		return ix.commitBlock(blk, "next block parent matches local head")
	}

	// 分叉：回滚到共同祖先，再重建第一个新区块。
	return ix.handleReorg(ctx, head, tip, next)
}

// ingestGenesis 摄入创世块（高度 0）。
func (ix *Indexer) ingestGenesis(ctx context.Context, tip *chain.Block) (bool, error) {
	blk, err := ix.source.BlockAt(ctx, 0, tip.Hash)
	if err != nil {
		return false, ix.classify("fetch genesis", 0, err)
	}
	if blk.Height != 0 {
		return false, ix.classify("fetch genesis", 0,
			&chain.DataError{Kind: chain.ErrKindFatal, Height: blk.Height,
				Reason: fmt.Sprintf("expected genesis at height 0, got height %d", blk.Height)})
	}
	return ix.commitBlock(blk, "cold start: indexing genesis")
}

// handleReorg 在不修改索引的前提下沿高度向下寻找共同祖先，校验深度上限，
// 随后原子回滚并重建祖先之后的第一个区块。
func (ix *Indexer) handleReorg(ctx context.Context, head store.BlockRecord, tip *chain.Block, next int64) (bool, error) {
	ancestor := int64(-1)
	localH := head.Height

	for h := localH; h >= 0; h-- {
		remote, err := ix.source.BlockAt(ctx, h, tip.Hash)
		if err != nil {
			de, isData := chain.AsDataError(err)
			// 新链在该高度无区块：视为该高度分叉，继续向下寻找祖先。
			if isData && de.Kind == chain.ErrKindFatal {
				ix.cfg.Logf("reorg probe: height=%d not present on new tip=%s basis=%s",
					h, tip.Hash, de.Reason)
				continue
			}
			return false, ix.classify(fmt.Sprintf("probe common ancestor at height=%d", h), h, err)
		}
		local, err := ix.store.BlockAt(h)
		if err != nil {
			return false, fmt.Errorf("indexer: read local block at height %d: %w", h, err)
		}
		ix.cfg.Logf("reorg probe: height=%d local=%s remote=%s basis=compare hashes",
			h, local.Hash, remote.Hash)
		if remote.Hash == local.Hash {
			ancestor = h
			break
		}
	}

	depth := head.Height - ancestor // ancestor=-1 时回滚深度含创世块
	if depth > ix.cfg.MaxReorgDepth {
		ix.metrics.RejectedReorgs.Add(1)
		return false, &ReorgTooDeepError{
			Depth:          depth,
			MaxDepth:       ix.cfg.MaxReorgDepth,
			OldTipHash:     head.Hash,
			OldTipHeight:   head.Height,
			NewTipHash:     tip.Hash,
			AncestorHeight: ancestor,
		}
	}

	if ancestor == head.Height {
		// 理论上不会发生（下一区块不衔接但本块一致时，必是父链问题），防御性处理。
		return false, fmt.Errorf("indexer: internal error: block at height %d matches but child does not connect", next)
	}

	// 原子回滚：删除 ancestor+1 及以上的区块与交易，绝不残留。
	if err := ix.store.TruncateFrom(ancestor + 1); err != nil {
		return false, fmt.Errorf("indexer: rollback truncate from %d: %w", ancestor+1, err)
	}
	ix.metrics.Rollbacks.Add(depth)
	ix.metrics.Reorgs.Add(1)
	ix.cfg.Logf("reorg applied: rolled back %d block(s) from height=%d to ancestor=%d oldtip=%s newtip=%s",
		depth, head.Height, ancestor, head.Hash, tip.Hash)

	// 重建祖先之后的第一个区块；其余区间由后续 Ingest 线性补齐。
	rebuild := ancestor + 1
	blk, err := ix.source.BlockAt(ctx, rebuild, tip.Hash)
	if err != nil {
		return false, ix.classify(fmt.Sprintf("rebuild block height=%d", rebuild), rebuild, err)
	}
	return ix.commitBlock(blk, fmt.Sprintf("reorg rebuild first block after ancestor=%d", ancestor))
}

// commitBlock 原子写入一个区块及其交易，并更新指标。
func (ix *Indexer) commitBlock(blk *chain.Block, basis string) (bool, error) {
	if existing, err := ix.store.BlockAt(blk.Height); err == nil && existing.Hash == blk.Hash {
		ix.metrics.DuplicateIngest.Add(1)
		ix.cfg.Logf("ingest idempotent: height=%d block=%s basis=same hash already indexed", blk.Height, blk.Hash)
		return false, nil
	}

	rec := store.BlockRecord{Height: blk.Height, Hash: blk.Hash, ParentHash: blk.ParentHash}
	txs := make([]store.TxRecord, 0, len(blk.Transactions))
	for i, tx := range blk.Transactions {
		txs = append(txs, store.TxRecord{
			TxHash:       tx.Hash,
			BlockHash:    blk.Hash,
			Height:       blk.Height,
			From:         tx.From,
			To:           tx.To,
			Value:        tx.Value,
			BlockOrdinal: i,
		})
	}
	if err := ix.store.ApplyBlock(rec, txs); err != nil {
		return false, fmt.Errorf("indexer: apply block height=%d hash=%s: %w", blk.Height, blk.Hash, err)
	}
	ix.metrics.BlocksIndexed.Add(1)
	ix.metrics.TxsIndexed.Add(int64(len(txs)))
	ix.cfg.Logf("ingest commit: height=%d block=%s parent=%s txs=%d basis=%s",
		blk.Height, blk.Hash, blk.ParentHash, len(txs), basis)
	return true, nil
}

// classify 把数据源错误归类为 temporary / fatal 并更新对应指标。
func (ix *Indexer) classify(action string, height int64, err error) error {
	de, ok := chain.AsDataError(err)
	if !ok {
		ix.metrics.FatalErrors.Add(1)
		return fmt.Errorf("indexer: %s: unclassified error: %w", action, err)
	}
	switch de.Kind {
	case chain.ErrKindTemporary:
		ix.metrics.TemporaryErrors.Add(1)
		ix.cfg.Logf("ingest temporary failure: action=%s height=%d basis=%s", action, height, de.Reason)
		return fmt.Errorf("indexer: %s: %w", action, err)
	default:
		ix.metrics.FatalErrors.Add(1)
		ix.cfg.Logf("ingest fatal failure: action=%s height=%d kind=%s basis=%s",
			action, height, de.Kind, de.Reason)
		return fmt.Errorf("indexer: %s: %w", action, err)
	}
}

// sleep 在 ctx 可取消的前提下等待退避时间。
func (ix *Indexer) sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close 释放底层存储资源。
func (ix *Indexer) Close() error { return ix.store.Close() }

// CatchUp 从已持久化进度持续摄入到当前链尖。
// 遇到临时失败按退避重试 MaxRetries 次；重试用尽或遇到不可恢复失败时停止并返回。
// 进度以已落盘的索引为准，因此重启后调用可从断点继续，不重复、不丢失。
func (ix *Indexer) CatchUp(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		advanced, err := ix.Ingest(ctx)
		if err != nil {
			de, ok := chain.AsDataError(err)
			if ok && de.Kind == chain.ErrKindTemporary {
				last := err
				for attempt := 1; attempt <= ix.cfg.MaxRetries; attempt++ {
					ix.cfg.Logf("catch-up retry: attempt=%d/%d height=%d", attempt, ix.cfg.MaxRetries, de.Height)
					if werr := ix.sleep(ctx, ix.cfg.RetryBackoff); werr != nil {
						return werr
					}
					advanced, last = ix.Ingest(ctx)
					if last == nil {
						break
					}
					de, ok = chain.AsDataError(last)
					if !ok || de.Kind != chain.ErrKindTemporary {
						return last
					}
				}
				if last != nil {
					return last
				}
			} else {
				return err
			}
		}
		if !advanced {
			return nil // 已到达链尖（或幂等重复摄入）
		}
	}
}

// BlockAt 按高度返回已索引区块；查询结论稳定，未索引返回 store.ErrNotFound。
func (ix *Indexer) BlockAt(height int64) (store.BlockRecord, error) {
	return ix.store.BlockAt(height)
}

// BlockByHash 按区块哈希返回已索引区块。
func (ix *Indexer) BlockByHash(hash string) (store.BlockRecord, error) {
	return ix.store.BlockByHash(hash)
}

// Tx 按交易标识返回已索引交易及其所属区块信息。
func (ix *Indexer) Tx(txHash string) (store.TxRecord, error) {
	return ix.store.Tx(txHash)
}

// Metrics 返回指标快照。
func (ix *Indexer) Metrics() Snapshot { return ix.metrics.snapshot() }
