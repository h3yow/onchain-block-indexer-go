package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// auditCfg 是测试默认开启的留痕配置（不限容量）。
func auditCfg() AuditConfig { return AuditConfig{Enabled: true} }

// mutateStore 在写锁内直接篡改底层映射并持久化，用于人为制造不一致。
func mutateStore(t *testing.T, st *Store, fn func()) {
	t.Helper()
	st.mu.Lock()
	defer st.mu.Unlock()
	fn()
	if err := st.persist(); err != nil {
		t.Fatalf("篡改后持久化失败: %v", err)
	}
}

// assertEqualFreshReplay 断言当前索引与「全新目录从头全量重放 ref 链」结论一致：
// 逐高度区块哈希、逐交易归属、总块数与总交易数全部相同。
func assertEqualFreshReplay(t *testing.T, ix *Indexer, ref Source, tip uint64) {
	t.Helper()
	snap := ix.store.Snapshot()
	if uint64(len(snap.Blocks)) != tip {
		t.Fatalf("区块数=%d,全量重放应为 %d", len(snap.Blocks), tip)
	}
	if uint64(len(snap.Txs)) != 2*tip {
		t.Fatalf("交易数=%d，全量重放应为 %d（无重复无悬挂）", len(snap.Txs), 2*tip)
	}
	for h := uint64(1); h <= tip; h++ {
		want, err := ref.FetchBlock(h)
		if err != nil {
			t.Fatalf("参照链拉取 %d: %v", h, err)
		}
		got, ok := snap.Blocks[h]
		if !ok || got.Hash != want.Hash {
			t.Fatalf("height=%d 索引=%s 全量重放=%s 不一致", h, got.Hash, want.Hash)
		}
		if got, ok := ix.store.BlockAt(h); !ok || got.Hash != want.Hash {
			t.Fatalf("BlockAt(%d)=%s 与全量重放 %s 不一致", h, got.Hash, want.Hash)
		}
		for _, tx := range want.Txs {
			it, ok := ix.store.TxByHash(tx.Hash)
			if !ok || it.BlockHeight != h || it.BlockHash != want.Hash {
				t.Fatalf("tx=%s 按交易查询结论与全量重放不一致: %+v ok=%v", tx.Hash, it, ok)
			}
		}
	}
}

// TestInspectHealthyReadOnly 健康状态下巡检只读：无任何问题，且不改动持久化状态；
// 索引滞后于数据源属于正常追赶，不得误判成脏数据。
func TestInspectHealthyReadOnly(t *testing.T) {
	src := NewSimChain("healthy", 10)
	dir := t.TempDir()
	ix, err := NewIndexer(Config{StoreDir: dir, Workers: 1, MaxReorgDepth: 8, PollBlocks: 6, Audit: auditCfg()}, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ckpt := filepath.Join(dir, "checkpoint.json")
	before, err := os.ReadFile(ckpt)
	if err != nil {
		t.Fatalf("读检查点: %v", err)
	}

	r, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	t.Logf("滞后巡检 indexTip=%d sourceTip=%d lag=%d healthy=%v 判定依据=链尖6/源10，未摄入区间是正常追赶",
		r.IndexTip, r.SourceTip, r.Lag, r.Healthy)
	if !r.Healthy || len(r.Issues) != 0 || r.Lag != 4 {
		t.Fatalf("滞后状态应判健康且 lag=4: %s", r.Summary())
	}
	after, _ := os.ReadFile(ckpt)
	if string(before) != string(after) {
		t.Fatalf("健康巡检改动了 checkpoint.json（必须只读）")
	}

	ix.cfg.PollBlocks = 10
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("追平 Run: %v", err)
	}
	r, err = ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	t.Logf("追平后巡检 indexTip=%d sourceTip=%d 判定依据=逐高度哈希与交易集合全部一致: %s",
		r.IndexTip, r.SourceTip, r.Summary())
	if !r.Healthy {
		t.Fatalf("追平后应健康: %s", r.Summary())
	}
}

// TestInspectIssueCategories 人为制造四类以上问题，巡检必须逐类分开列出。
func TestInspectIssueCategories(t *testing.T) {
	src := NewSimChain("cats", 10)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	st := ix.store
	snap0 := st.Snapshot()
	b4 := snap0.Blocks[4]
	b6 := snap0.Blocks[6]

	mutateStore(t, st, func() {
		// 缺口：删除高度 4 区块及其交易（链尖仍为 10）。
		delete(st.blocksByHei, 4)
		delete(st.blockHeiByHsh, b4.Hash)
		for _, tx := range b4.Txs {
			delete(st.txs, tx.Hash)
		}
		// 内容不符：篡改高度 6 区块哈希（同时导致高度 7 父链断裂）。
		b6.Hash = "tampered-h6"
		st.blocksByHei[6] = b6
		delete(st.blockHeiByHsh, snap0.Blocks[6].Hash)
		st.blockHeiByHsh["tampered-h6"] = 6
		// 多余高度：高出数据源链尖 10。
		st.blocksByHei[11] = Block{Height: 11, Hash: "extra-h11", ParentHash: "x"}
		// 多余交易：不属于任何区块。
		st.txs["ghost-tx"] = IndexedTx{Tx: Tx{Hash: "ghost-tx", From: "a", To: "b"}}
	})

	r, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	for _, is := range r.Issues {
		t.Logf("发现 %s", is.String())
	}
	t.Logf("巡检结论: %s", r.Summary())
	c := r.CountByKind()
	for _, k := range []IssueKind{IssueMissing, IssueMismatch, IssueParentBroken, IssueExtraHeight, IssueExtraTx} {
		if c[k] == 0 {
			t.Fatalf("缺少问题类别 %s 的发现", k)
		}
	}
	if r.FirstMismatch != 4 || r.MaxIndexedHeight != 11 {
		t.Fatalf("首个不一致=%d（应4）最大高度=%d（应11）", r.FirstMismatch, r.MaxIndexedHeight)
	}
	if got := r.IssuesOf(IssueMissing); len(got) != 1 || got[0].Height != 4 {
		t.Fatalf("缺失类定位错误: %+v", got)
	}
}

// TestRepairEqualsFullReplay 发现分叉不一致后就地自愈，结果必须与清空全量重放一致；
// 重复修复保持幂等，无重复记录与计数漂移。
func TestRepairEqualsFullReplay(t *testing.T) {
	src := NewSimChain("heal", 12)
	dir := t.TempDir()
	ix, err := NewIndexer(Config{StoreDir: dir, Workers: 1, MaxReorgDepth: 16, Audit: auditCfg()}, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	old9, _ := ix.store.BlockAt(9)
	old12, _ := ix.store.BlockAt(12)
	t.Logf("分叉前 height=9 hash=%s height=12 hash=%s", old9.Hash, old12.Hash)

	src.Fork(9) // 共同祖先 8，深度 12-8=4 <= 16
	before, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	t.Logf("修复前巡检 mismatch=%d firstMismatch=%d 判定依据=9..12 同高度哈希对不上: %s",
		before.CountByKind()[IssueMismatch], before.FirstMismatch, before.Summary())
	if before.Healthy || before.FirstMismatch != 9 || before.CountByKind()[IssueMismatch] != 4 {
		t.Fatalf("修复前应在高度9发现内容不符: %s", before.Summary())
	}

	report, res, err := ix.Repair(ctx)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	t.Logf("修复结果 reverted=%d rebuilt=%d purgedHeight=%d purgedTx=%d newTip=%d 判定依据=回滚到8再重建9..12",
		res.Reverted, res.Rebuilt, res.PurgedHeights, res.PurgedTxs, res.NewTip)
	if res.HealthyBefore || res.Reverted != 4 || res.Rebuilt != 4 || res.NewTip != 12 {
		t.Fatalf("修复动作量不符: %+v report=%s", res, report.Summary())
	}

	// 与全新目录全量重放（同样分叉）逐高度逐交易比对。
	refSrc := NewSimChain("heal", 12)
	refSrc.Fork(9)
	refIx, err := NewIndexer(Config{StoreDir: t.TempDir(), Workers: 1, MaxReorgDepth: 16}, refSrc)
	if err != nil {
		t.Fatalf("ref NewIndexer: %v", err)
	}
	if err := refIx.Run(ctx); err != nil {
		t.Fatalf("ref Run: %v", err)
	}
	ref := NewSimChain("heal", 12)
	ref.Fork(9)
	assertEqualFreshReplay(t, ix, ref, 12)
	// 旧分支交易不得再按交易查到。
	if _, ok := ix.store.TxByHash(old9.Txs[0].Hash); ok {
		t.Fatalf("旧分支交易 %s 修复后仍可查", old9.Txs[0].Hash)
	}

	// 留痕：本次重组放弃的 9..12 应可按高度/交易/区块反查。
	if n := ix.Audit().Len(); n != 4 {
		t.Fatalf("留痕条数=%d，应4", n)
	}
	if e := ix.Audit().ByTx(old12.Txs[1].Hash); len(e) != 1 || e[0].Height != 12 || e[0].BranchID == "" {
		t.Fatalf("按交易反查被放弃分支失败: %+v", e)
	}

	// 幂等：再修两次，必须零改动、计数不漂移。
	for i := 0; i < 2; i++ {
		r2, res2, err := ix.Repair(ctx)
		if err != nil {
			t.Fatalf("第%d次重复 Repair: %v", i+2, err)
		}
		t.Logf("第%d次重复修复 healthyBefore=%v 改动=(%d,%d,%d,%d) 判定依据=巡检已健康须零改动",
			i+2, res2.HealthyBefore, res2.Reverted, res2.Rebuilt, res2.PurgedHeights, res2.PurgedTxs)
		if !res2.HealthyBefore || res2.Reverted+res2.Rebuilt+res2.PurgedHeights+res2.PurgedTxs != 0 {
			t.Fatalf("健康状态重复修复产生了改动: %+v report=%s", res2, r2.Summary())
		}
	}
	assertEqualFreshReplay(t, ix, ref, 12)
	if n := ix.Audit().Len(); n != 4 {
		t.Fatalf("重复修复导致留痕漂移: %d", n)
	}
}

// TestRepairRejectedWhenTooDeep 自愈回滚超 MaxReorgDepth：明确拒绝，索引保持原样，不写留痕。
func TestRepairRejectedWhenTooDeep(t *testing.T) {
	src := NewSimChain("deepheal", 10)
	dir := t.TempDir()
	ix, err := NewIndexer(Config{StoreDir: dir, Workers: 1, MaxReorgDepth: 2, Audit: auditCfg()}, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	before, beforeHash, _ := ix.store.Tip()
	b10, _ := ix.store.BlockAt(10)

	src.Fork(2) // 共同祖先 1，深度 10-1=9 > 2
	r, _, err := ix.Repair(ctx)
	t.Logf("超限自愈 firstMismatch=%d 判定依据=深度9 > 上限2，拒绝且保持原样: err=%v", r.FirstMismatch, err)
	if err == nil || KindOf(err) != KindReorgTooDeep {
		t.Fatalf("应 KindReorgTooDeep, got %v", err)
	}
	after, afterHash, _ := ix.store.Tip()
	if after != before || afterHash != beforeHash {
		t.Fatalf("被拒绝的修复改动了链尖: (%d,%s)->(%d,%s)", before, beforeHash, after, afterHash)
	}
	if got, _ := ix.store.BlockAt(10); got.Hash != b10.Hash {
		t.Fatalf("原链尖区块被改动")
	}
	if ix.Audit().Len() != 0 {
		t.Fatalf("拒绝修复不得产生留痕: %d", ix.Audit().Len())
	}
	// 巡检仍能发现问题（拒绝不影响只读能力）。
	r2, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if r2.Healthy || r2.FirstMismatch != 2 {
		t.Fatalf("拒绝后巡检结论异常: %s", r2.Summary())
	}
}

// TestRepairTamperedCategories 缺口/父链断裂/多余高度/多余交易混合污染：
// 修复后同样与全量重放一致。
func TestRepairTamperedCategories(t *testing.T) {
	src := NewSimChain("cats", 10)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	st := ix.store
	snap0 := st.Snapshot()
	b4 := snap0.Blocks[4]
	b6 := snap0.Blocks[6]
	mutateStore(t, st, func() {
		delete(st.blocksByHei, 4)
		delete(st.blockHeiByHsh, b4.Hash)
		for _, tx := range b4.Txs {
			delete(st.txs, tx.Hash)
		}
		b6.Hash = "tampered-h6"
		st.blocksByHei[6] = b6
		delete(st.blockHeiByHsh, snap0.Blocks[6].Hash)
		st.blockHeiByHsh["tampered-h6"] = 6
		st.blocksByHei[11] = Block{Height: 11, Hash: "extra-h11", ParentHash: "x"}
		st.txs["ghost-tx"] = IndexedTx{Tx: Tx{Hash: "ghost-tx", From: "a", To: "b"}}
	})

	r, res, err := ix.Repair(ctx)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	t.Logf("混合污染修复 reverted=%d rebuilt=%d purgedHeight=%d purgedTx=%d 判定依据=%s",
		res.Reverted, res.Rebuilt, res.PurgedHeights, res.PurgedTxs, r.Summary())
	if res.Reverted != 6 || res.Rebuilt != 7 || res.PurgedHeights != 1 || res.PurgedTxs != 1 {
		t.Fatalf("修复动作量不符: %+v", res)
	}
	assertEqualFreshReplay(t, ix, NewSimChain("cats", 10), 10)
	if _, ok := ix.store.TxByHash("ghost-tx"); ok {
		t.Fatalf("多余交易未清理")
	}
	if _, ok := ix.store.BlockAt(11); ok {
		t.Fatalf("多余高度未清理")
	}
}

// TestAuditTrailLifecycle 重组留痕：常规重组写入、重启可查、按高度/交易/区块
// 反查被放弃分支，且被放弃区块不会借留痕回到常规查询；容量裁剪生效。
func TestAuditTrailLifecycle(t *testing.T) {
	src := NewSimChain("aud", 10)
	dir := t.TempDir()
	cfg := Config{StoreDir: dir, Workers: 1, MaxReorgDepth: 8, Audit: AuditConfig{Enabled: true}}
	ix, err := NewIndexer(cfg, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	old8, _ := ix.store.BlockAt(8)
	old9, _ := ix.store.BlockAt(9)
	old10, _ := ix.store.BlockAt(10)

	src.Fork(8) // 放弃 8、9、10，共同祖先 7
	ix2, err := NewIndexer(cfg, src)
	if err != nil {
		t.Fatalf("重启 NewIndexer: %v", err)
	}
	ix2.next = 8
	if err := ix2.Run(ctx); err != nil {
		t.Fatalf("重组 Run: %v", err)
	}
	if n := ix2.Audit().Len(); n != 3 {
		t.Fatalf("留痕条数=%d，应3（高度8/9/10）", n)
	}
	byH := ix2.Audit().ByHeight(9)
	if len(byH) != 1 || byH[0].BlockHash != old9.Hash || byH[0].ReorgBase != 7 {
		t.Fatalf("按高度反查留痕失败: %+v", byH)
	}
	byTx := ix2.Audit().ByTx(old10.Txs[0].Hash)
	if len(byTx) != 1 || byTx[0].BranchID != byH[0].BranchID {
		t.Fatalf("按交易反查分支失败: %+v", byTx)
	}
	byBlk := ix2.Audit().ByBlock(old8.Hash)
	if len(byBlk) != 1 || byBlk[0].Height != 8 {
		t.Fatalf("按区块反查失败: %+v", byBlk)
	}
	t.Logf("留痕 height=9 block=%s branch=%s base=7 newTip=10 判定依据=该块属于被放弃旧分支",
		old9.Hash, byH[0].BranchID)

	// 旁路隔离：常规查询只返回新分支，旧区块/旧交易不可见。
	new9, _ := ix2.store.BlockAt(9)
	if new9.Hash == old9.Hash {
		t.Fatalf("被放弃区块借留痕回到常规查询")
	}
	if _, ok := ix2.store.TxByHash(old9.Txs[0].Hash); ok {
		t.Fatalf("被放弃交易借留痕回到常规查询")
	}

	// 重启后留痕仍在。
	ix3, err := NewIndexer(cfg, src)
	if err != nil {
		t.Fatalf("二次重启: %v", err)
	}
	if n := ix3.Audit().Len(); n != 3 {
		t.Fatalf("重启后留痕丢失: %d", n)
	}
	if e := ix3.Audit().ByHeight(10); len(e) != 1 || e[0].BlockHash != old10.Hash {
		t.Fatalf("重启后按高度反查失败: %+v", e)
	}
	if ix3.Met.AuditRecords.Load() != 0 {
		t.Fatalf("留痕指标不应在重启恢复时重复计数")
	}
}

// TestAuditRetention 条数上限与高度窗口裁剪：只清留痕、按规则保留、并发安全。
func TestAuditRetention(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenAuditLog(dir, AuditConfig{Enabled: true, MaxEntries: 3})
	if err != nil {
		t.Fatalf("OpenAuditLog: %v", err)
	}
	mkBlock := func(h uint64) Block {
		return Block{Height: h, Hash: fmt.Sprintf("h%d", h), ParentHash: "p",
			Txs: []Tx{{Hash: fmt.Sprintf("t%d", h), From: "f", To: "to"}}}
	}
	if _, err := a.Append([]Block{mkBlock(10), mkBlock(11)}, 9, 11); err != nil {
		t.Fatalf("append1: %v", err)
	}
	if _, err := a.Append([]Block{mkBlock(12), mkBlock(13)}, 11, 13); err != nil {
		t.Fatalf("append2: %v", err)
	}
	all := a.All()
	if len(all) != 3 || all[0].ID != 2 {
		t.Fatalf("条数上限裁剪错误: len=%d firstID=%d（应保留ID2/3/4）", len(all), all[0].ID)
	}
	if e := a.ByHeight(10); len(e) != 0 {
		t.Fatalf("最旧留痕 height=10 应已被裁剪")
	}
	if e := a.ByTx("t13"); len(e) != 1 {
		t.Fatalf("保留留痕按交易反查失败")
	}

	// 高度窗口保留。
	dir2 := t.TempDir()
	a2, err := OpenAuditLog(dir2, AuditConfig{Enabled: true, RetainBlocks: 2})
	if err != nil {
		t.Fatalf("OpenAuditLog2: %v", err)
	}
	if _, err := a2.Append([]Block{mkBlock(10), mkBlock(11)}, 9, 11); err != nil {
		t.Fatalf("append a2-1: %v", err)
	}
	if _, err := a2.Append([]Block{mkBlock(5), mkBlock(6)}, 4, 11); err != nil {
		t.Fatalf("append a2-2: %v", err)
	}
	if n := a2.Len(); n != 2 {
		t.Fatalf("高度窗口裁剪后条数=%d，应2（只留10/11）", n)
	}
	// 手动 Prune 幂等且不报错。
	if n, err := a2.Prune(); err != nil || n != 0 {
		t.Fatalf("重复 Prune: n=%d err=%v", n, err)
	}

	// 并发写入不损坏留痕。
	dir3 := t.TempDir()
	a3, err := OpenAuditLog(dir3, AuditConfig{Enabled: true, MaxEntries: 50})
	if err != nil {
		t.Fatalf("OpenAuditLog3: %v", err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				h := uint64(100 + g*20 + i)
				_, _ = a3.Append([]Block{mkBlock(h)}, h-1, h)
			}
		}(g)
	}
	wg.Wait()
	if n := a3.Len(); n != 50 {
		t.Fatalf("并发写入后裁剪条数=%d，应50", n)
	}
	if ids := a3.All(); ids[0].ID >= ids[len(ids)-1].ID {
		t.Fatalf("留痕编号顺序损坏")
	}
}

// TestInspectRepairConcurrentWithIngestion 巡检/修复与摄入同时进行：
// 线性推进期间巡检永远判健康；分叉修复期间读不到中间破损（无缺口/断链/多余），
// 最终结论与分叉链全量重放一致。
func TestInspectRepairConcurrentWithIngestion(t *testing.T) {
	src := NewSimChain("race", 60)
	dir := t.TempDir()
	ix, err := NewIndexer(Config{StoreDir: dir, Workers: 8, MaxReorgDepth: 16,
		PollBlocks: 40, Audit: auditCfg()}, src)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("初始摄入到40: %v", err)
	}
	ix.cfg.PollBlocks = 60

	var wg sync.WaitGroup
	var failMu sync.Mutex
	var failures []string
	addFail := func(f string) {
		failMu.Lock()
		failures = append(failures, f)
		failMu.Unlock()
	}

	// 阶段一：线性摄入 41..60 与高频巡检并发。任何已提交快照都必须判健康。
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 60; j++ {
				r, err := ix.Inspect(ctx)
				if err != nil {
					addFail(fmt.Sprintf("巡检出错: %v", err))
					return
				}
				if !r.Healthy {
					addFail(fmt.Sprintf("线性推进期间误判脏数据: %s", r.Summary()))
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ix.Run(ctx); err != nil {
			addFail(fmt.Sprintf("阶段一摄入: %v", err))
		}
	}()
	wg.Wait()
	if tip, _, _ := ix.store.Tip(); tip != 60 {
		t.Fatalf("阶段一链尖=%d，应60", tip)
	}

	// 阶段二：高度 55 分叉，摄入重组与自愈修复、巡检同时进行。
	src.Fork(55) // 深度 60-54=6 <= 16
	ix.next = 55
	var runErr, repairErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ix.Run(ctx); err != nil {
			failMu.Lock()
			runErr = err
			failMu.Unlock()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, _, err := ix.Repair(ctx); err != nil {
			failMu.Lock()
			repairErr = err
			failMu.Unlock()
		}
	}()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, err := ix.Inspect(ctx)
				if err != nil {
					addFail(fmt.Sprintf("阶段二巡检出错: %v", err))
					return
				}
				// 中间状态绝不允许出现缺口/断链/多余数据；内容不符只能发生在分叉点55及以上。
				for _, is := range r.Issues {
					switch is.Kind {
					case IssueMissing, IssueParentBroken, IssueExtraHeight, IssueExtraTx:
						addFail(fmt.Sprintf("读到摄入中间破损: %s", is.String()))
						return
					case IssueMismatch:
						if is.Height < 55 {
							addFail(fmt.Sprintf("分叉点以下被误判不符: %s", is.String()))
							return
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	if runErr != nil {
		addFail(fmt.Sprintf("阶段二摄入返回: %v", runErr))
	}
	if repairErr != nil {
		addFail(fmt.Sprintf("阶段二修复返回: %v", repairErr))
	}
	if len(failures) > 0 {
		for _, f := range failures {
			t.Error(f)
		}
	}

	// 最终结论与分叉链全量重放一致，块数/交易数无漂移。
	snap := ix.store.Snapshot()
	if len(snap.Blocks) != 60 || len(snap.Txs) != 120 {
		t.Fatalf("最终计数 blocks=%d txs=%d，应60/120", len(snap.Blocks), len(snap.Txs))
	}
	ref := NewSimChain("race", 60)
	ref.Fork(55)
	for h := uint64(1); h <= 60; h++ {
		want, _ := ref.FetchBlock(h)
		got, ok := ix.store.BlockAt(h)
		if !ok || got.Hash != want.Hash {
			t.Fatalf("height=%d got=%s want=%s", h, got.Hash, want.Hash)
		}
	}
	t.Logf("并发巡检/修复/摄入结束 tip=60 判定依据=逐高度哈希等于分叉链，无缺口/断链/多余记录")
}
