package indexer

import (
	"context"
	"testing"
)

func newTestIndexer(t *testing.T, src Source, workers int, maxReorg uint64) (*Indexer, string) {
	t.Helper()
	dir := t.TempDir()
	ix, err := NewIndexer(Config{
		StoreDir:      dir,
		Workers:       workers,
		MaxReorgDepth: maxReorg,
	}, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	return ix, dir
}

// TestNormalProgression 正常推进：按高度顺序摄入，查询结论稳定。
func TestNormalProgression(t *testing.T) {
	src := NewSimChain("normal", 10)
	ix, _ := newTestIndexer(t, src, 1, 8)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tipH, tipHash, ok := ix.Store().Tip()
	t.Logf("链尖 height=%d hash=%s 判定依据=数据源长度10，应全部摄入", tipH, tipHash)
	if !ok || tipH != 10 {
		t.Fatalf("tip = (%d,%s,%v), want height 10", tipH, tipHash, ok)
	}
	for h := uint64(1); h <= 10; h++ {
		b, ok := ix.Store().BlockAt(h)
		if !ok {
			t.Fatalf("高度 %d 缺失", h)
		}
		if h > 1 {
			prev, _ := ix.Store().BlockAt(h - 1)
			if b.ParentHash != prev.Hash {
				t.Fatalf("高度 %d 父哈希断裂", h)
			}
		}
		for _, tx := range b.Txs {
			it, ok := ix.Store().TxByHash(tx.Hash)
			if !ok || it.BlockHeight != h || it.BlockHash != b.Hash {
				t.Fatalf("tx %s 索引不一致: %+v ok=%v", tx.Hash, it, ok)
			}
		}
	}
	t.Logf("指标 applied=%d txs=%d 判定依据=10块×2交易", ix.Met.BlocksApplied.Load(), ix.Met.TxsApplied.Load())
	if ix.Met.BlocksApplied.Load() != 10 || ix.Met.TxsApplied.Load() != 20 {
		t.Fatalf("指标不符 applied=%d txs=%d dup=%d", ix.Met.BlocksApplied.Load(), ix.Met.TxsApplied.Load(), ix.Met.Duplicates.Load())
	}
}

// TestDuplicateIngestionIdempotent 重复摄入同一高度：幂等，无重复记录。
func TestDuplicateIngestionIdempotent(t *testing.T) {
	src := NewSimChain("dup", 6)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	b5, _ := ix.Store().BlockAt(5)
	// 直接重复提交高度 5（同哈希）。
	if err := ix.commit(ctx, b5); err != nil {
		t.Fatalf("重复 commit: %v", err)
	}
	t.Logf("重复摄入 height=%d hash=%s 判定依据=同高度同哈希，幂等跳过", b5.Height, b5.Hash)
	if ix.Met.Duplicates.Load() != 1 {
		t.Fatalf("Duplicates = %d, want 1", ix.Met.Duplicates.Load())
	}
	// 存储层幂等：同区块再次 ApplyBlock 不产生重复。
	applied, err := ix.Store().ApplyBlock(b5)
	if err != nil || applied {
		t.Fatalf("ApplyBlock 重复: applied=%v err=%v", applied, err)
	}
	// 记录数不变：6 块、12 交易。
	if ix.Met.BlocksApplied.Load() != 6 || ix.Met.TxsApplied.Load() != 12 {
		t.Fatalf("重复摄入产生了新记录 applied=%d txs=%d dup=%d", ix.Met.BlocksApplied.Load(), ix.Met.TxsApplied.Load(), ix.Met.Duplicates.Load())
	}
	for _, tx := range b5.Txs {
		if it, ok := ix.Store().TxByHash(tx.Hash); !ok || it.BlockHeight != 5 {
			t.Fatalf("tx %s 记录异常", tx.Hash)
		}
	}
}

// TestShallowReorg 浅重组：回滚到共同祖先并重建，被抛弃区块与交易无残留。
func TestShallowReorg(t *testing.T) {
	src := NewSimChain("reorg", 10)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	old9, _ := ix.Store().BlockAt(9)
	old10, _ := ix.Store().BlockAt(10)
	t.Logf("分叉前 height=9 hash=%s / height=10 hash=%s", old9.Hash, old10.Hash)

	src.Fork(8)
	// 重启流水线（同一存储目录），从持久化进度继续并检测重组。
	ix2, err := NewIndexer(Config{StoreDir: ix.cfg.StoreDir, Workers: 1, MaxReorgDepth: 8}, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	// 让流水线重新评估链尖区间：回退 next 以触发对分叉高度的比对。
	ix2.next = 8
	if err := ix2.Run(ctx); err != nil {
		t.Fatalf("重组 Run: %v", err)
	}
	t.Logf("重组判定 reorgs=%d reverted=%d 判定依据=分叉高度8，共同祖先7，深度=10-7=3 <= 上限8",
		ix2.Met.Reorgs.Load(), ix2.Met.BlocksReverted.Load())
	if ix2.Met.Reorgs.Load() != 1 || ix2.Met.BlocksReverted.Load() != 3 {
		t.Fatalf("重组指标不符: reorgs=%d reverted=%d", ix2.Met.Reorgs.Load(), ix2.Met.BlocksReverted.Load())
	}
	// 被抛弃的区块与交易不得残留。
	for _, old := range []Block{old9, old10} {
		for _, tx := range old.Txs {
			if _, ok := ix2.Store().TxByHash(tx.Hash); ok {
				t.Fatalf("被抛弃交易 %s (height=%d) 仍残留在索引中", tx.Hash, old.Height)
			}
		}
	}
	// 新分支区块就位且哈希不同于旧分支。
	for h := uint64(8); h <= 10; h++ {
		nb, ok := ix2.Store().BlockAt(h)
		if !ok {
			t.Fatalf("重组后高度 %d 缺失", h)
		}
		t.Logf("重建 height=%d newHash=%s", nb.Height, nb.Hash)
	}
	new9, _ := ix2.Store().BlockAt(9)
	if new9.Hash == old9.Hash {
		t.Fatalf("高度 9 未被新分支替换")
	}
	if tipH, _, _ := ix2.Store().Tip(); tipH != 10 {
		t.Fatalf("重组后链尖 = %d, want 10", tipH)
	}
}

// TestReorgTooDeep 超限重组：明确拒绝并报告，索引保持原状。
func TestReorgTooDeep(t *testing.T) {
	src := NewSimChain("deepreorg", 10)
	ix, _ := newTestIndexer(t, src, 1, 1) // 上限 1
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	before, beforeHash, _ := ix.Store().Tip()

	src.Fork(8) // 深度 = 10-7 = 3 > 上限 1
	ix.next = 8
	err := ix.Run(ctx)
	t.Logf("超限重组 height=%d 判定依据=深度3 > 上限1，必须拒绝: err=%v", 8, err)
	if err == nil || KindOf(err) != KindReorgTooDeep {
		t.Fatalf("应返回 KindReorgTooDeep, got %v", err)
	}
	after, afterHash, _ := ix.Store().Tip()
	if after != before || afterHash != beforeHash {
		t.Fatalf("超限重组后索引被改动: (%d,%s) -> (%d,%s)", before, beforeHash, after, afterHash)
	}
	if _, ok := ix.Store().BlockAt(10); !ok {
		t.Fatalf("原链尖区块丢失")
	}
}

// TestConcurrentIngestion 并发摄入：无竞态、无中间破损，结论与串行一致。
func TestConcurrentIngestion(t *testing.T) {
	src := NewSimChain("concurrent", 200)
	ix, _ := newTestIndexer(t, src, 8, 16)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tipH, _, _ := ix.Store().Tip()
	t.Logf("并发摄入 workers=8 链尖 height=%d 判定依据=200 块全部按序提交", tipH)
	if tipH != 200 {
		t.Fatalf("tip = %d, want 200", tipH)
	}
	if ix.Met.BlocksApplied.Load() != 200 || ix.Met.TxsApplied.Load() != 400 {
		t.Fatalf("指标不符 applied=%d txs=%d dup=%d", ix.Met.BlocksApplied.Load(), ix.Met.TxsApplied.Load(), ix.Met.Duplicates.Load())
	}
	// 与串行结果逐块比对哈希。
	serial := NewSimChain("concurrent", 200)
	for h := uint64(1); h <= 200; h++ {
		want, _ := serial.FetchBlock(h)
		got, ok := ix.Store().BlockAt(h)
		if !ok || got.Hash != want.Hash {
			t.Fatalf("高度 %d 哈希不一致: got=%s want=%s", h, got.Hash, want.Hash)
		}
	}
}

// badSource 返回字段缺失的非法区块。
type badSource struct{ tip uint64 }

func (s badSource) Tip() uint64 { return s.tip }
func (s badSource) FetchBlock(h uint64) (Block, error) {
	return Block{Height: h, ParentHash: "p"}, nil // Hash 缺失
}

// TestInvalidDataClassified 非法数据：不静默丢弃，归类为 KindInvalidData。
func TestInvalidDataClassified(t *testing.T) {
	ix, _ := newTestIndexer(t, badSource{tip: 3}, 1, 4)
	err := ix.Run(context.Background())
	t.Logf("非法区块 height=1 判定依据=block hash 缺失，归类=%v", KindOf(err))
	if err == nil || KindOf(err) != KindInvalidData {
		t.Fatalf("应返回 KindInvalidData, got %v", err)
	}
	if ix.Met.InvalidBlocks.Load() != 1 {
		t.Fatalf("InvalidBlocks = %d, want 1", ix.Met.InvalidBlocks.Load())
	}
	if _, _, ok := ix.Store().Tip(); ok {
		t.Fatalf("非法区块被写入索引")
	}
}

// TestTransientFailureRetried 临时失败：正确归类、自动重试、不中断同步。
func TestTransientFailureRetried(t *testing.T) {
	src := NewSimChain("transient", 8)
	src.InjectTransient(3)
	ix, _ := newTestIndexer(t, src, 1, 8)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("临时失败不应终止同步: %v", err)
	}
	tipH, _, _ := ix.Store().Tip()
	t.Logf("临时失败 height=1 重试3次后链尖 height=%d 判定依据=KindTransientSource 可重试", tipH)
	if tipH != 8 {
		t.Fatalf("tip = %d, want 8", tipH)
	}
	if ix.Met.TransientErrs.Load() != 3 {
		t.Fatalf("TransientErrs = %d, want 3", ix.Met.TransientErrs.Load())
	}
}

// TestFatalFailureAndRecovery 不可恢复失败：归类为 KindFatalSource 并停止；
// 故障排除后重启，从持久化进度继续，不重复不丢失。
func TestFatalFailureAndRecovery(t *testing.T) {
	src := NewSimChain("fatal", 10)
	src.InjectFatalAt(6)
	ix, dir := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	err := ix.Run(ctx)
	t.Logf("不可恢复失败 height=6 判定依据=KindFatalSource 不重试: err=%v", err)
	if err == nil || KindOf(err) != KindFatalSource {
		t.Fatalf("应返回 KindFatalSource, got %v", err)
	}
	if ix.Met.FatalErrs.Load() != 1 {
		t.Fatalf("FatalErrs = %d, want 1", ix.Met.FatalErrs.Load())
	}
	if tipH, _, _ := ix.Store().Tip(); tipH != 5 {
		t.Fatalf("失败后链尖 = %d, want 5", tipH)
	}

	// 故障排除，重启：从高度 6 继续。
	src2 := NewSimChain("fatal", 10)
	ix2, err := NewIndexer(Config{StoreDir: dir, Workers: 1, MaxReorgDepth: 8}, src2)
	if err != nil {
		t.Fatalf("重启 NewIndexer: %v", err)
	}
	t.Logf("重启恢复 next=%d 判定依据=检查点链尖5，下一高度6", ix2.Next())
	if ix2.Next() != 6 {
		t.Fatalf("恢复进度 = %d, want 6", ix2.Next())
	}
	if err := ix2.Run(ctx); err != nil {
		t.Fatalf("恢复后 Run: %v", err)
	}
	if tipH, _, _ := ix2.Store().Tip(); tipH != 10 {
		t.Fatalf("恢复后链尖 = %d, want 10", tipH)
	}
	// 不重复：第二次运行只应用 5 个新区块。
	if got := ix2.Met.BlocksApplied.Load(); got != 5 {
		t.Fatalf("恢复后重复摄入: applied=%d, want 5", got)
	}
	if got := ix2.Met.Duplicates.Load(); got != 0 {
		t.Fatalf("恢复后出现重复: %d", got)
	}
	for h := uint64(1); h <= 10; h++ {
		if _, ok := ix2.Store().BlockAt(h); !ok {
			t.Fatalf("恢复后高度 %d 丢失", h)
		}
	}
}
