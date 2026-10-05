package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// 本文件覆盖新增能力：
//  1. 健康状态下巡检只读、不改任何状态
//  2. 发现不一致后自愈重建，结论与全量重放一致（按高度/按交易）
//  3. 修复重复执行幂等（无重复记录、无计数漂移）
//  4. 超限重组时修复被拒绝且索引原样
//  5. 留痕重启后仍可按高度/交易反查，且不影响常规查询
//  6. 留痕容量控制与并发安全
//  7. 巡检/修复与并发摄入同时进行无竞态、无误判

func fullReplayStore(t *testing.T, seed string, length, forkAt uint64) *Store {
	t.Helper()
	src := NewSimChain(seed, length)
	if forkAt > 0 {
		src.Fork(forkAt)
	}
	ix, err := NewIndexer(Config{
		StoreDir:      t.TempDir(),
		Workers:       1,
		MaxReorgDepth: length,
	}, src)
	if err != nil {
		t.Fatalf("全量重放 NewIndexer: %v", err)
	}
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("全量重放 Run: %v", err)
	}
	return ix.Store()
}

func assertStoresEqual(t *testing.T, got, want *Store, length uint64, scope string) {
	t.Helper()
	for h := uint64(1); h <= length; h++ {
		gb, gok := got.BlockAt(h)
		wb, wok := want.BlockAt(h)
		if gok != wok || gb.Hash != wb.Hash {
			t.Fatalf("[%s] 高度 %d 按高度查询不一致: got=(%s,%v) want=(%s,%v)",
				scope, h, gb.Hash, gok, wb.Hash, wok)
		}
		for _, tx := range wb.Txs {
			git, gok := got.TxByHash(tx.Hash)
			wit, wok := want.TxByHash(tx.Hash)
			if gok != wok || git != wit {
				t.Fatalf("[%s] 交易 %s 按交易查询不一致: got=(%+v,%v) want=(%+v,%v)",
					scope, tx.Hash, git, gok, wit, wok)
			}
		}
	}
}

func recordCounts(s *Store) (blocks, txs int) {
	snap := s.Snapshot()
	return len(snap.blocks), len(snap.txs)
}

// TestInspectHealthyReadOnly 健康索引：巡检报告干净，且不改动任何持久化状态。
func TestInspectHealthyReadOnly(t *testing.T) {
	src := NewSimChain("inspect-ok", 12)
	ix, dir := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ckpt := filepath.Join(dir, "checkpoint.json")
	before, err := os.ReadFile(ckpt)
	if err != nil {
		t.Fatalf("读检查点: %v", err)
	}
	b0, b1 := recordCounts(ix.Store())
	applied0 := ix.Met.BlocksApplied.Load()

	rep, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	t.Logf("健康巡检 height范围=[%d,%d] 链尖=%d 数据源链尖=%d 判定依据=12块全部按序摄入，应无问题",
		rep.CheckedFrom, rep.CheckedTo, rep.TipHeight, rep.SourceTip)
	if !rep.Healthy || len(rep.Issues) != 0 {
		t.Fatalf("健康索引不应有问题: %+v", rep.Issues)
	}
	for name, list := range map[string][]Issue{
		"missing": rep.Missing(), "mismatch": rep.Mismatches(), "parent": rep.ParentBreaks(),
		"extra": rep.ExtraBlocks(), "orphan": rep.OrphanTxs(),
	} {
		if len(list) != 0 {
			t.Fatalf("类别 %s 应为空: %+v", name, list)
		}
	}
	after, err := os.ReadFile(ckpt)
	if err != nil {
		t.Fatalf("读检查点: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("巡检改动了检查点文件")
	}
	a0, a1 := recordCounts(ix.Store())
	if a0 != b0 || a1 != b1 || ix.Met.BlocksApplied.Load() != applied0 {
		t.Fatalf("巡检改动了内存状态: blocks %d->%d txs %d->%d applied %d->%d",
			b0, a0, b1, a1, applied0, ix.Met.BlocksApplied.Load())
	}
	t.Logf("巡检前后 blocks=%d txs=%d 判定依据=检查点字节与记录计数均不变", a0, a1)
}

// TestInspectClassifiesIssues 巡检把缺高/内容不符/父链断裂/游离交易分类列出。
func TestInspectClassifiesIssues(t *testing.T) {
	src := NewSimChain("inspect-bad", 10)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := ix.Store()

	// 删除高度 5：制造缺口（高度缺口）。
	s.mu.Lock()
	b5 := s.blocksByHei[5]
	for _, tx := range b5.Txs {
		delete(s.txs, tx.Hash)
	}
	delete(s.blockHeiByHsh, b5.Hash)
	delete(s.blocksByHei, 5)
	// 注入一条无区块引用的游离交易。
	s.txs["ghost-tx"] = IndexedTx{
		Tx:          Tx{Hash: "ghost-tx", From: "f", To: "t", Amount: 1},
		BlockHeight: 99,
		BlockHash:   "ghost-block",
	}
	if err := s.persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	s.mu.Unlock()
	t.Logf("注入脏数据: 删除 height=5 块=%s，注入游离交易 ghost-tx", b5.Hash)

	rep, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if rep.Healthy {
		t.Fatalf("应检测到不一致")
	}
	if len(rep.Missing()) != 1 || rep.Missing()[0].Height != 5 {
		t.Fatalf("缺高分类错误: %+v", rep.Missing())
	}
	if len(rep.OrphanTxs()) != 1 || rep.OrphanTxs()[0].TxHash != "ghost-tx" {
		t.Fatalf("游离交易分类错误: %+v", rep.OrphanTxs())
	}
	t.Logf("分类结论 missing=%v orphanTx=%v 判定依据=高度5无区块、ghost-tx无区块引用",
		rep.Missing()[0].Height, rep.OrphanTxs()[0].TxHash)
}

// TestHealForkEqualsFullReplay 分叉后自愈：按高度/按交易结论与全量重放一致。
func TestHealForkEqualsFullReplay(t *testing.T) {
	const length, forkAt = uint64(10), uint64(6)
	src := NewSimChain("heal-fork", length)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	old6, _ := ix.Store().BlockAt(6)
	src.Fork(forkAt) // 数据源在高度 6 起分叉，索引仍是旧分支
	t.Logf("分叉点=%d 旧分支 height=6 块=%s 判定依据=巡检应发现 6..10 mismatch", forkAt, old6.Hash)

	rep, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if rep.Healthy || len(rep.Mismatches()) != 5 {
		t.Fatalf("应发现 5 个 mismatch, got healthy=%v issues=%d", rep.Healthy, len(rep.Mismatches()))
	}
	for _, m := range rep.Mismatches() {
		t.Logf("巡检 mismatch height=%d 索引块=%s 数据源块=%s", m.Height, m.BlockHash, m.SourceHash)
	}

	hr, err := ix.Heal(ctx)
	if err != nil {
		t.Fatalf("Heal: %v", err)
	}
	t.Logf("自愈结论 firstBad=%d ancestor=%d reverted=%d rebuilt=%d 判定依据=回滚到共同祖先5再顺序重建",
		hr.FirstBadHeight, hr.AncestorHeight, hr.Reverted, hr.Rebuilt)
	if !hr.Healed || hr.AncestorHeight != forkAt-1 || hr.Reverted != 5 || hr.Rebuilt != 5 {
		t.Fatalf("自愈范围异常: %+v", hr)
	}

	want := fullReplayStore(t, "heal-fork", length, forkAt)
	assertStoresEqual(t, ix.Store(), want, length, "heal-vs-replay")
	tipH, _, _ := ix.Store().Tip()
	if tipH != length {
		t.Fatalf("自愈后链尖 = %d, want %d", tipH, length)
	}

	// 留痕：自愈回滚的旧分支 6..10 应被记录，原因 heal。
	evs := ix.Audit().Events()
	if len(evs) != 1 || evs[0].Reason != AbandonByHeal || len(evs[0].Records) != 5 {
		t.Fatalf("自愈留痕异常: %+v", evs)
	}
	t.Logf("自愈留痕 branch=%s 高度区间=[%d,%d]",
		evs[0].BranchID, evs[0].Records[0].Height, evs[0].Records[len(evs[0].Records)-1].Height)
}

// TestHealGapEqualsFullReplay 中间缺高后自愈：同样等价全量重放。
func TestHealGapEqualsFullReplay(t *testing.T) {
	const length = uint64(10)
	src := NewSimChain("heal-gap", length)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 直接抹掉高度 4、5（模拟持久化残缺）。
	s := ix.Store()
	s.mu.Lock()
	for _, h := range []uint64{4, 5} {
		b := s.blocksByHei[h]
		for _, tx := range b.Txs {
			delete(s.txs, tx.Hash)
		}
		delete(s.blockHeiByHsh, b.Hash)
		delete(s.blocksByHei, h)
	}
	if err := s.persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	s.mu.Unlock()

	rep, err := ix.Inspect(ctx)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(rep.Missing()) != 2 {
		t.Fatalf("应发现 2 个缺高, got %+v", rep.Missing())
	}
	hr, err := ix.Heal(ctx)
	if err != nil {
		t.Fatalf("Heal: %v", err)
	}
	t.Logf("缺高自愈 firstBad=%d ancestor=%d reverted=%d rebuilt=%d",
		hr.FirstBadHeight, hr.AncestorHeight, hr.Reverted, hr.Rebuilt)
	if hr.AncestorHeight != 3 {
		t.Fatalf("共同祖先应为 3, got %d", hr.AncestorHeight)
	}
	want := fullReplayStore(t, "heal-gap", length, 0)
	assertStoresEqual(t, ix.Store(), want, length, "heal-gap")
}

// TestHealIdempotent 修复重复执行幂等：无重复记录、无计数漂移。
func TestHealIdempotent(t *testing.T) {
	const length, forkAt = uint64(10), uint64(7)
	src := NewSimChain("heal-idem", length)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	src.Fork(forkAt)

	for i := 1; i <= 3; i++ {
		hr, err := ix.Heal(ctx)
		if err != nil {
			t.Fatalf("第 %d 次 Heal: %v", i, err)
		}
		blocks, txs := recordCounts(ix.Store())
		if i == 1 {
			if !hr.Healed {
				t.Fatalf("首次修复应实际执行")
			}
		} else {
			if hr.Healed {
				t.Fatalf("第 %d 次修复应是空操作", i)
			}
		}
		if blocks != int(length) || txs != int(2*length) {
			t.Fatalf("第 %d 次修复后记录漂移 blocks=%d txs=%d", i, blocks, txs)
		}
		t.Logf("第%d次自愈 healed=%v blocks=%d txs=%d 判定依据=重复修复不产生重复记录",
			i, hr.Healed, blocks, txs)
	}
	want := fullReplayStore(t, "heal-idem", length, forkAt)
	assertStoresEqual(t, ix.Store(), want, length, "heal-idem")
}

// TestHealRejectedWhenTooDeep 自愈深度超上限：拒绝并保持索引原样。
func TestHealRejectedWhenTooDeep(t *testing.T) {
	const length, forkAt = uint64(10), uint64(8)
	src := NewSimChain("heal-deep", length)
	ix, _ := newTestIndexer(t, src, 1, 1) // 上限 1；分叉深度=10-7=3
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	old8, _ := ix.Store().BlockAt(8)
	src.Fork(forkAt)

	before := ix.Store().Snapshot()
	hr, err := ix.Heal(ctx)
	t.Logf("超限自愈 firstBad=%d 判定依据=深度3 > 上限1，必须拒绝: err=%v", forkAt, err)
	if err == nil || KindOf(err) != KindReorgTooDeep {
		t.Fatalf("应返回 KindReorgTooDeep, got %v", err)
	}
	if hr.Healed || hr.Reverted != 0 {
		t.Fatalf("被拒绝时不应有任何回滚: %+v", hr)
	}
	after := ix.Store().Snapshot()
	if len(after.blocks) != len(before.blocks) || len(after.txs) != len(before.txs) {
		t.Fatalf("拒绝后记录数变化: blocks %d->%d txs %d->%d",
			len(before.blocks), len(after.blocks), len(before.txs), len(after.txs))
	}
	if cur, ok := ix.Store().BlockAt(8); !ok || cur.Hash != old8.Hash {
		t.Fatalf("拒绝后高度 8 被改动: (%s,%v)", cur.Hash, ok)
	}
}

// TestAuditTrailRestartLookup 留痕重启后仍可查，且不影响常规按高度/交易查询。
func TestAuditTrailRestartLookup(t *testing.T) {
	const length, forkAt = uint64(10), uint64(8)
	src := NewSimChain("audit-restart", length)
	ix, dir := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	old9, _ := ix.Store().BlockAt(9)
	oldTx := old9.Txs[0].Hash
	src.Fork(forkAt)
	ix.next = forkAt
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("重组 Run: %v", err)
	}

	// 重启前：按高度/按交易都能反查到被放弃分支。
	if v := ix.Audit().LookupByHeight(9); len(v) != 1 || v[0].BlockHash != old9.Hash ||
		v[0].Reason != AbandonByReorg {
		t.Fatalf("重启前按高度反查留痕异常: %+v", v)
	}
	tv, ok := ix.Audit().LookupByTx(oldTx)
	if !ok || tv.BlockHash != old9.Hash || tv.Height != 9 {
		t.Fatalf("重启前按交易反查留痕异常: %+v ok=%v", tv, ok)
	}
	t.Logf("留痕反查 height=9 旧块=%s tx=%s branch=%s 判定依据=重组回滚8..10旧分支",
		old9.Hash, oldTx, tv.BranchID)

	// 常规查询不受影响：旧块/旧交易消失，新分支就位。
	if cur, ok := ix.Store().BlockAt(9); !ok || cur.Hash == old9.Hash {
		t.Fatalf("常规按高度查询混入被放弃区块: %+v", cur)
	}
	if _, ok := ix.Store().TxByHash(oldTx); ok {
		t.Fatalf("常规按交易查询混入被放弃交易 %s", oldTx)
	}

	// 重启：重新打开同一目录，留痕仍在。
	ix2, err := NewIndexer(Config{StoreDir: dir, Workers: 1, MaxReorgDepth: 8}, src)
	if err != nil {
		t.Fatalf("重启 NewIndexer: %v", err)
	}
	if v := ix2.Audit().LookupByHeight(9); len(v) != 1 || v[0].BlockHash != old9.Hash {
		t.Fatalf("重启后留痕丢失: %+v", v)
	}
	if tv, ok := ix2.Audit().LookupByTx(oldTx); !ok || tv.BlockHash != old9.Hash {
		t.Fatalf("重启后按交易反查失败: %+v ok=%v", tv, ok)
	}
	t.Logf("重启后留痕仍可查 eventId=%d 高度=%d 判定依据=audit-trail.json 独立持久化",
		tv.EventID, tv.Height)
	// 常规查询依旧干净。
	if _, ok := ix2.Store().TxByHash(oldTx); ok {
		t.Fatalf("重启后被放弃交易混入常规查询")
	}
}

// mkEvent 构造一个含单区块记录的测试事件。
func mkEvent(id, height uint64, hashSeed string) AbandonedEvent {
	return AbandonedEvent{
		EventID: id,
		Reason:  AbandonByReorg,
		Records: []AbandonedRecord{{
			Height:     height,
			BlockHash:  fmt.Sprintf("blk-%s-%d", hashSeed, height),
			ParentHash: fmt.Sprintf("parent-%d", height),
			TxHashes:   []string{fmt.Sprintf("tx-%s-%d", hashSeed, height)},
		}},
	}
}

// TestAuditTrailRetention 留痕容量控制：条数上限、按高度清理、重启保留。
func TestAuditTrailRetention(t *testing.T) {
	dir := t.TempDir()
	tr, err := OpenAuditTrail(dir, AuditPolicy{})
	if err != nil {
		t.Fatalf("OpenAuditTrail: %v", err)
	}
	for id := uint64(1); id <= 5; id++ {
		if err := tr.AppendEvent(mkEvent(id, id+2, "cap")); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
	}
	if got := len(tr.Events()); got != 5 {
		t.Fatalf("无上限策略应保留 5 条, got %d", got)
	}
	// 按条数上限清理：先进先出留最新 2 条。
	if _, err := tr.Prune(AuditPolicy{MaxEvents: 2}); err != nil {
		t.Fatalf("Prune MaxEvents: %v", err)
	}
	evs := tr.Events()
	if len(evs) != 2 || evs[0].EventID != 4 || evs[1].EventID != 5 {
		t.Fatalf("条数上限清理异常: %+v", evs)
	}
	t.Logf("条数上限清理后保留 eventId=[%d,%d] 判定依据=先进先出", evs[0].EventID, evs[1].EventID)
	if _, ok := tr.LookupByTx("tx-cap-3"); ok {
		t.Fatalf("被清理事件的交易不应可反查")
	}

	// 按高度清理：追加一条含 3 个高度的事件，清掉 <7 的记录。
	multi := AbandonedEvent{Reason: AbandonByHeal, Records: []AbandonedRecord{
		{Height: 5, BlockHash: "b5", TxHashes: []string{"t5"}},
		{Height: 6, BlockHash: "b6", TxHashes: []string{"t6"}},
		{Height: 7, BlockHash: "b7", TxHashes: []string{"t7"}},
	}}
	if err := tr.AppendEvent(multi); err != nil {
		t.Fatalf("AppendEvent multi: %v", err)
	}
	removed, err := tr.Prune(AuditPolicy{PruneBelowHeight: 7})
	if err != nil || removed != 3 {
		t.Fatalf("按高度清理 removed=%d err=%v（应清掉 multi 的 5,6 与 id=4 事件的高度6）", removed, err)
	}
	if v := tr.LookupByHeight(5); len(v) != 0 {
		t.Fatalf("高度 5 的留痕应已清理: %+v", v)
	}
	// 高度 7 保留两条：id=5 事件的 blk-cap-7 与 multi 事件的 b7。
	if v := tr.LookupByHeight(7); len(v) != 2 {
		t.Fatalf("高度 7 的留痕应保留 2 条: %+v", v)
	}
	t.Logf("按高度清理 removed=%d 判定依据=PruneBelowHeight=7", removed)

	// AutoPrune：追加即自动裁剪到上限。
	tr2, err := OpenAuditTrail(t.TempDir(), AuditPolicy{MaxEvents: 2, AutoPrune: true})
	if err != nil {
		t.Fatalf("OpenAuditTrail: %v", err)
	}
	for id := uint64(1); id <= 4; id++ {
		if err := tr2.AppendEvent(mkEvent(id, id, "auto")); err != nil {
			t.Fatalf("auto append: %v", err)
		}
	}
	if got := len(tr2.Events()); got != 2 {
		t.Fatalf("AutoPrune 应保留 2 条, got %d", got)
	}

	// 重启保留。
	tr3, err := OpenAuditTrail(dir, AuditPolicy{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	v := tr3.LookupByHeight(7)
	found := false
	for _, view := range v {
		if view.BlockHash == "b7" {
			found = true
		}
	}
	if !found {
		t.Fatalf("重启后留痕丢失 b7: %+v", v)
	}
	if v0 := tr3.LookupByHeight(5); len(v0) != 0 {
		t.Fatalf("重启后高度 5 留痕应已清理: %+v", v0)
	}
}

// TestAuditTrailConcurrentSafe 留痕并发追加/清理/反查不写坏（配合 -race）。
func TestAuditTrailConcurrentSafe(t *testing.T) {
	tr, err := OpenAuditTrail(t.TempDir(), AuditPolicy{})
	if err != nil {
		t.Fatalf("OpenAuditTrail: %v", err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := uint64(g*1000 + i + 1)
				_ = tr.AppendEvent(mkEvent(id, uint64(i%50+1), fmt.Sprintf("g%d", g)))
			}
		}(g)
	}
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				select {
				case <-stop:
					return
				default:
				}
				tr.LookupByHeight(uint64(i%50 + 1))
				tr.LookupByTx(fmt.Sprintf("tx-g%d-%d", i%4, i%50+1))
				_, _ = tr.Prune(AuditPolicy{MaxEvents: 30})
				_ = tr.Events()
				runtime.Gosched()
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	// 收敛后再无并发写入：裁剪到一个稳定上限并校验索引一致性。
	if _, err := tr.Prune(AuditPolicy{MaxEvents: 500}); err != nil {
		t.Fatalf("收敛 Prune: %v", err)
	}
	for _, ev := range tr.Events() {
		for _, rec := range ev.Records {
			if v := tr.LookupByHeight(rec.Height); len(v) == 0 {
				t.Fatalf("记录索引损坏: height %d 反查为空", rec.Height)
			}
			if tv, ok := tr.LookupByTx(rec.TxHashes[0]); !ok || tv.BlockHash != rec.BlockHash {
				t.Fatalf("记录索引损坏: tx %s 反查=%+v ok=%v", rec.TxHashes[0], tv, ok)
			}
		}
	}
	t.Logf("并发留痕收敛后 events=%d 判定依据=高度/交易反查索引全部自洽", len(tr.Events()))
}

// TestPruneDoesNotAffectIndex 清理留痕不破坏主索引正确性。
func TestPruneDoesNotAffectIndex(t *testing.T) {
	src := NewSimChain("audit-prune", 8)
	ix, _ := newTestIndexer(t, src, 1, 8)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	src.Fork(6)
	ix.next = 6
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("reorg Run: %v", err)
	}
	if removed, err := ix.Audit().Prune(AuditPolicy{PruneBelowHeight: 100}); err != nil || removed == 0 {
		t.Fatalf("Prune removed=%d err=%v", removed, err)
	}
	if len(ix.Audit().Events()) != 0 {
		t.Fatalf("全部留痕应被清理")
	}
	// 主索引完好且继续幂等摄入。
	if h, _, _ := ix.Store().Tip(); h != 8 {
		t.Fatalf("清理留痕后链尖异常: %d", h)
	}
	b8, _ := ix.Store().BlockAt(8)
	if err := ix.commit(ctx, b8); err != nil {
		t.Fatalf("清理后幂等提交失败: %v", err)
	}
	if ix.Met.Duplicates.Load() == 0 {
		t.Fatalf("应计一次幂等重复")
	}
}

// TestInspectHealConcurrentWithIngestion 巡检/自愈与摄入并发：无竞态、无误判。
func TestInspectConcurrentWithIngestion(t *testing.T) {
	src := NewSimChain("race-ingest", 200)
	ix, _ := newTestIndexer(t, src, 8, 16)
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 巡检协程：摄入进行中索引的已提交前缀必须始终与数据源一致，
	// 任何缺高/父链断裂/游离交易都意味着读到了中间状态（误判）。
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rep, err := ix.Inspect(ctx)
				if err != nil {
					return
				}
				for _, is := range rep.Issues {
					if is.Kind == IssueMissing || is.Kind == IssueParentBroken || is.Kind == IssueOrphanTx {
						t.Errorf("巡检协程%d 读到摄入中间状态: kind=%s height=%d", g, is.Kind, is.Height)
						return
					}
				}
			}
		}(g)
	}
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(stop)
	wg.Wait()
	if h, _, _ := ix.Store().Tip(); h != 200 {
		t.Fatalf("并发摄入链尖 = %d, want 200", h)
	}
	t.Logf("并发摄入+巡检 链尖=200 判定依据=全程无 missing/parent-broken/orphan 误判")
}

func TestHealConcurrentWithInspect(t *testing.T) {
	const length, forkAt = uint64(120), uint64(60)
	src := NewSimChain("race-heal", length)
	ix, _ := newTestIndexer(t, src, 4, 128)
	ctx := context.Background()
	if err := ix.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	src.Fork(forkAt)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 巡检协程：整个回滚-重建被 stateMu 包住，外部只能看到“修复前的旧
	// 分支不匹配”或“修复后的健康状态”，不可能看到缺高/父链断裂等中间态。
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rep, err := ix.Inspect(ctx)
				if err != nil {
					return
				}
				for _, is := range rep.Issues {
					if is.Kind != IssueMismatch {
						t.Errorf("自愈期间巡检读到非预期类别 kind=%s height=%d", is.Kind, is.Height)
						return
					}
					if is.Height < forkAt {
						t.Errorf("分叉点之前不应有问题: height=%d", is.Height)
						return
					}
				}
			}
		}(g)
	}
	hr, err := ix.Heal(ctx)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("Heal: %v", err)
	}
	t.Logf("并发自愈 ancestor=%d reverted=%d rebuilt=%d 判定依据=巡检只见旧分支mismatch或健康态",
		hr.AncestorHeight, hr.Reverted, hr.Rebuilt)
	want := fullReplayStore(t, "race-heal", length, forkAt)
	assertStoresEqual(t, ix.Store(), want, length, "race-heal")
}
