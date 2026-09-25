package indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/onchain-block-indexer-go/internal/chain"
	"github.com/h3yow/onchain-block-indexer-go/internal/store"
)

type testEnv struct {
	src  *chain.SimSource
	st   store.Store
	logs *bytes.Buffer
	ix   *Indexer
	mtx  *sync.Mutex
}

func newTestEnv(t *testing.T, maxReorg int64, st store.Store) *testEnv {
	t.Helper()
	var buf bytes.Buffer
	mtx := &sync.Mutex{}
	src := chain.NewSimSource()
	ix := New(Config{
		MaxReorgDepth: maxReorg,
		MaxRetries:    5,
		RetryBackoff:  time.Millisecond,
		Logf: func(format string, args ...any) {
			mtx.Lock()
			defer mtx.Unlock()
			fmt.Fprintf(&buf, format+"\n", args...)
		},
	}, src, st)
	return &testEnv{src: src, st: st, logs: &buf, ix: ix, mtx: mtx}
}

func (e *testEnv) dumpLogs(t *testing.T) {
	t.Helper()
	e.mtx.Lock()
	defer e.mtx.Unlock()
	t.Logf("=== indexer logs ===\n%s", e.logs.String())
}

// TestNormalAdvance 按高度顺序摄入，每块/每交易均入库且可按标识查询。
func TestNormalAdvance(t *testing.T) {
	env := newTestEnv(t, 3, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	blocks := env.src.ExtendN(3) // 高度 1..3（创世块由索引器自行摄入）

	if err := env.ix.CatchUp(ctx); err != nil {
		t.Fatalf("catch up: %v", err)
	}
	head, err := env.st.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Height != 3 || head.Hash != blocks[2].Hash {
		t.Fatalf("head = height=%d hash=%s, want height=3 hash=%s", head.Height, head.Hash, blocks[2].Hash)
	}
	for _, b := range blocks {
		got, err := env.ix.BlockByHash(b.Hash)
		if err != nil {
			t.Fatalf("query block %s: %v", b.Hash, err)
		}
		if got.Height != b.Height {
			t.Fatalf("block %s indexed height=%d want %d", b.Hash, got.Height, b.Height)
		}
		txHash := b.Transactions[0].Hash
		tr, err := env.ix.Tx(txHash)
		if err != nil {
			t.Fatalf("query tx %s: %v", txHash, err)
		}
		if tr.BlockHash != b.Hash {
			t.Fatalf("tx %s belongs to %s want %s", txHash, tr.BlockHash, b.Hash)
		}
		t.Logf("verified height=%d block=%s tx=%s basis=lookups stable", b.Height, b.Hash, txHash)
	}
	m := env.ix.Metrics()
	if m.BlocksIndexed != 4 { // 创世 + 3
		t.Fatalf("blocks indexed=%d want 4", m.BlocksIndexed)
	}
	if m.TxsIndexed != 3 {
		t.Fatalf("txs indexed=%d want 3", m.TxsIndexed)
	}
}

// TestDuplicateIngest 重复摄入同一高度幂等，不产生重复记录并计入指标。
func TestDuplicateIngest(t *testing.T) {
	env := newTestEnv(t, 3, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	env.src.ExtendN(2)
	if err := env.ix.CatchUp(ctx); err != nil {
		t.Fatalf("first catch up: %v", err)
	}
	baseline := env.ix.Metrics().DuplicateIngest
	// 数据源没有新区块：反复摄入必须全部幂等。
	for i := 0; i < 5; i++ {
		advanced, err := env.ix.Ingest(ctx)
		if err != nil {
			t.Fatalf("duplicate ingest %d: %v", i, err)
		}
		if advanced {
			t.Fatalf("duplicate ingest %d must not advance", i)
		}
	}
	txs, err := env.st.(*store.MemStore).TxsAt(1)
	if err != nil {
		t.Fatalf("txs at 1: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected exactly 1 tx at height 1 after duplicate ingest, got %d", len(txs))
	}
	m := env.ix.Metrics()
	if m.DuplicateIngest-baseline != 5 {
		t.Fatalf("duplicate metric delta=%d want 5", m.DuplicateIngest-baseline)
	}
	t.Logf("duplicate ingest idempotent: height=1 block=%s tx-count=1 dup-metric=%d",
		txs[0].BlockHash, m.DuplicateIngest)
}

// TestShallowReorg 浅重组：回滚到共同祖先并重建，抛弃区块/交易无残留。
func TestShallowReorg(t *testing.T) {
	env := newTestEnv(t, 5, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	old := env.src.ExtendN(5) // 高度 1..5
	if err := env.ix.CatchUp(ctx); err != nil {
		t.Fatalf("initial catch up: %v", err)
	}
	abandonedHashes := map[string]bool{old[2].Hash: true, old[3].Hash: true, old[4].Hash: true}
	abandonedTxs := map[string]bool{
		old[2].Transactions[0].Hash: true,
		old[3].Transactions[0].Hash: true,
		old[4].Transactions[0].Hash: true,
	}

	// 在高度 2 之后分叉（共同祖先是高度 2），新链 4 个区块（链尖高度 6）。
	newTip := env.src.Reorg(2, 4)
	if err := env.ix.CatchUp(ctx); err != nil {
		t.Fatalf("catch up after reorg: %v", err)
	}

	head, _ := env.st.Head()
	if head.Hash != newTip || head.Height != 6 {
		t.Fatalf("head after reorg = %s@%d want %s@6", head.Hash, head.Height, newTip)
	}
	for h := int64(3); h <= 6; h++ {
		b, err := env.ix.BlockAt(h)
		if err != nil {
			t.Fatalf("missing rebuilt block at %d: %v", h, err)
		}
		if abandonedHashes[b.Hash] {
			t.Fatalf("abandoned block hash %s remains at height %d", b.Hash, h)
		}
	}
	for txHash := range abandonedTxs {
		if _, err := env.ix.Tx(txHash); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("abandoned tx %s still queryable: err=%v", txHash, err)
		}
	}
	// 共同祖先及之前的区块保持不变。
	ancestor, err := env.ix.BlockAt(2)
	if err != nil {
		t.Fatalf("ancestor query: %v", err)
	}
	if ancestor.Hash != old[1].Hash {
		t.Fatalf("ancestor mutated: got %s want %s", ancestor.Hash, old[1].Hash)
	}
	m := env.ix.Metrics()
	if m.Reorgs != 1 || m.Rollbacks != 3 {
		t.Fatalf("reorg metrics=%+v, want reorgs=1 rollbacks=3", m)
	}
	t.Logf("shallow reorg consistent: ancestor-height=2 ancestor=%s new-tip=%s rollbacks=%d",
		ancestor.Hash, newTip, m.Rollbacks)
	env.dumpLogs(t)
}

// TestReorgExceedsLimit 超限重组必须明确拒绝，索引保持拒绝前原样。
func TestReorgExceedsLimit(t *testing.T) {
	env := newTestEnv(t, 2, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	old := env.src.ExtendN(5)
	if err := env.ix.CatchUp(ctx); err != nil {
		t.Fatalf("initial catch up: %v", err)
	}

	// 共同祖先高度 0 => 回滚深度 5 > 上限 2；新链更长（6 个块，链尖高度 6）。
	env.src.Reorg(0, 6)
	err := env.ix.CatchUp(ctx)
	if err == nil {
		t.Fatalf("expected reorg rejection, got nil")
	}
	var tooDeep *ReorgTooDeepError
	if !errors.As(err, &tooDeep) {
		t.Fatalf("expected *ReorgTooDeepError, got %T: %v", err, err)
	}
	if tooDeep.Depth != 5 || tooDeep.MaxDepth != 2 || tooDeep.AncestorHeight != 0 {
		t.Fatalf("unexpected rejection detail: %+v", tooDeep)
	}

	// 拒绝后索引状态原样：旧链尖仍在，指标记录一次拒绝。
	head, _ := env.st.Head()
	if head.Hash != old[4].Hash || head.Height != 5 {
		t.Fatalf("state mutated after rejected reorg: head=%s@%d", head.Hash, head.Height)
	}
	for h := int64(0); h <= 5; h++ {
		if _, err := env.ix.BlockAt(h); err != nil {
			t.Fatalf("block at %d lost after rejected reorg: %v", h, err)
		}
	}
	if m := env.ix.Metrics(); m.RejectedReorgs != 1 {
		t.Fatalf("rejected metric=%d want 1", m.RejectedReorgs)
	}
	t.Logf("reorg rejected as required: %s", tooDeep.Error())
	env.dumpLogs(t)
}

// TestConcurrentIngest 多个 goroutine 并发摄入：最终状态一致、无重复、无中间破损。
func TestConcurrentIngest(t *testing.T) {
	env := newTestEnv(t, 5, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	const total = 20
	env.src.ExtendN(total)

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := env.ix.CatchUp(ctx); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent catch up: %v", err)
	}

	head, _ := env.st.Head()
	if head.Height != total {
		t.Fatalf("head height=%d want %d", head.Height, total)
	}
	// 每个高度必须恰好存在一个区块、一笔交易（创世除外）。
	txCount := 0
	for h := int64(0); h <= total; h++ {
		if _, err := env.ix.BlockAt(h); err != nil {
			t.Fatalf("block at %d: %v", h, err)
		}
		txs, err := env.st.(*store.MemStore).TxsAt(h)
		if err != nil {
			t.Fatalf("txs at %d: %v", h, err)
		}
		txCount += len(txs)
	}
	m := env.ix.Metrics()
	if m.BlocksIndexed != int64(total+1) {
		t.Fatalf("blocks indexed=%d want %d (no duplicates)", m.BlocksIndexed, total+1)
	}
	if m.TxsIndexed != int64(total) || txCount != total {
		t.Fatalf("txs indexed=%d stored=%d want %d", m.TxsIndexed, txCount, total)
	}
	t.Logf("concurrent ingest consistent: head-height=%d blocks=%d txs=%d dup-skips=%d",
		head.Height, m.BlocksIndexed, m.TxsIndexed, m.DuplicateIngest)
}

// TestTemporaryFailureRetried 临时失败被归类并在重试后成功，进度不丢。
func TestTemporaryFailureRetried(t *testing.T) {
	env := newTestEnv(t, 3, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	env.src.ExtendN(3)
	env.src.FailTemporary(2, 2) // 高度 2 前两次读取临时失败

	if err := env.ix.CatchUp(ctx); err != nil {
		t.Fatalf("catch up with temporary failures: %v", err)
	}
	head, _ := env.st.Head()
	if head.Height != 3 {
		t.Fatalf("head height=%d want 3 after recovery", head.Height)
	}
	m := env.ix.Metrics()
	if m.TemporaryErrors < 2 {
		t.Fatalf("temporary error metric=%d want >=2", m.TemporaryErrors)
	}
	t.Logf("temporary failures handled: classified-count=%d final height=%d block=%s",
		m.TemporaryErrors, head.Height, head.Hash)
	env.dumpLogs(t)
}

// TestFatalFailureDistinct 不可恢复失败与临时失败区分，且不推进进度。
func TestFatalFailureDistinct(t *testing.T) {
	env := newTestEnv(t, 3, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	env.src.ExtendN(3)
	env.src.FailFatal(2)
	err := env.ix.CatchUp(ctx)
	if err == nil {
		t.Fatalf("expected fatal error")
	}
	de, ok := chain.AsDataError(err)
	if !ok {
		t.Fatalf("error %v cannot be classified", err)
	}
	if de.Kind != chain.ErrKindFatal {
		t.Fatalf("want fatal kind, got %s", de.Kind)
	}
	head, _ := env.st.Head()
	if head.Height != 1 {
		t.Fatalf("progress must stop at height 1, got %d", head.Height)
	}
	m := env.ix.Metrics()
	if m.FatalErrors < 1 || m.TemporaryErrors != 0 {
		t.Fatalf("metrics wrong: fatal=%d temporary=%d", m.FatalErrors, m.TemporaryErrors)
	}
	t.Logf("fatal failure classified distinctly: height=%d kind=%s basis=%s stopped-at=%d",
		de.Height, de.Kind, de.Reason, head.Height)
}

// TestInvalidBlockNotSilentlyDropped 非法字段必须给出可区分的 validation 原因。
func TestInvalidBlockNotSilentlyDropped(t *testing.T) {
	env := newTestEnv(t, 3, store.NewMemStore())
	defer env.ix.Close()
	ctx := context.Background()

	blocks := env.src.ExtendN(3)
	// 替换高度 1 的区块（非链尖）：交易缺失 From 地址，必须报 validation 错误。
	bad := &chain.Block{
		Height:     1,
		Hash:       blocks[0].Hash, // 保留原哈希与父子衔接，仅交易字段非法
		ParentHash: chain.GenesisHash,
		Transactions: []chain.Transaction{{
			Hash:  "0x" + strings.Repeat("b", 64),
			From:  "",
			To:    "0x" + strings.Repeat("c", 40),
			Value: 1,
		}},
	}
	env.src.OverrideBlock(1, bad)

	err := env.ix.CatchUp(ctx)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	de, ok := chain.AsDataError(err)
	if !ok || de.Kind != chain.ErrKindValidation {
		t.Fatalf("want validation error, got %v", err)
	}
	if !strings.Contains(de.Reason, "invalid from address") {
		t.Fatalf("reason should point at invalid from address, got %q", de.Reason)
	}
	t.Logf("invalid block rejected: height=%d block=%s kind=%s basis=%s",
		de.Height, bad.Hash, de.Kind, de.Reason)
}

// TestRestartResumesWithoutRepeating 使用文件快照模拟重启：从持久化进度继续。
func TestRestartResumesWithoutRepeating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	ctx := context.Background()

	st1, err := store.OpenFileStore(path)
	if err != nil {
		t.Fatalf("open store 1: %v", err)
	}
	src := chain.NewSimSource()
	blocks := src.ExtendN(3)

	ix1 := New(Config{MaxReorgDepth: 3}, src, st1)
	if err := ix1.CatchUp(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	m1 := ix1.Metrics()
	_ = ix1.Close()

	// 重启：同一数据源、全新索引器实例、已存在的快照。
	st2, err := store.OpenFileStore(path)
	if err != nil {
		t.Fatalf("open store 2: %v", err)
	}
	ix2 := New(Config{MaxReorgDepth: 3}, src, st2)
	if err := ix2.CatchUp(ctx); err != nil {
		t.Fatalf("second run: %v", err)
	}
	head, _ := st2.Head()
	if head.Height != 3 || head.Hash != blocks[2].Hash {
		t.Fatalf("after restart head=%s@%d want %s@3", head.Hash, head.Height, blocks[2].Hash)
	}
	m2 := ix2.Metrics()
	if m2.BlocksIndexed != 0 {
		t.Fatalf("restart must not re-index blocks, got %d", m2.BlocksIndexed)
	}
	_ = ix2.Close()
	t.Logf("restart resumed without repeating: run1 blocks=%d txs=%d; run2 new-blocks=%d head=%s@%d basis=persisted snapshot",
		m1.BlocksIndexed, m1.TxsIndexed, m2.BlocksIndexed, head.Hash, head.Height)
}
