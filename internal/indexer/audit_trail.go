package indexer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// 本文件实现重组旁路留痕（AuditTrail）：记录每次重组被放弃的分支高度与
// 区块标识，独立持久化（audit-trail.json），不影响常规按高度/按交易查询。
//
// 并发约定：AuditTrail 自带互斥锁；Store 在持写锁时调用它，锁序固定为
// Store.mu -> AuditTrail.mu，不会反向获取。

// AbandonReason 描述留痕产生的原因。
type AbandonReason string

const (
	// AbandonByReorg 常规摄入过程中发生重组而放弃旧分支。
	AbandonByReorg AbandonReason = "reorg"
	// AbandonByHeal 巡检自愈回滚而放弃旧分支。
	AbandonByHeal AbandonReason = "heal"
)

// AbandonedRecord 是一个被放弃区块的留痕条目。
type AbandonedRecord struct {
	Height     uint64   `json:"height"`
	BlockHash  string   `json:"blockHash"`
	ParentHash string   `json:"parentHash"`
	TxHashes   []string `json:"txHashes"`
}

// AbandonedEvent 是一次重组产生的一组放弃记录。
type AbandonedEvent struct {
	EventID     uint64            `json:"eventId"`
	BranchID    string            `json:"branchId"`
	Reason      AbandonReason     `json:"reason"`
	AbandonedAt int64             `json:"abandonedAt"`
	Records     []AbandonedRecord `json:"records"`
}

// AbandonedRecordView 是按高度/交易反查时返回的留痕视图（含所属事件上下文）。
type AbandonedRecordView struct {
	EventID     uint64
	BranchID    string
	Reason      AbandonReason
	AbandonedAt int64
	Height      uint64
	BlockHash   string
	ParentHash  string
	TxHashes    []string
}

// AuditPolicy 控制留痕的保留与清理。零值表示永久保留、不自动清理。
type AuditPolicy struct {
	// MaxEvents 保留的重组事件条数上限（按事件序号先进先出），<=0 不限。
	MaxEvents int
	// PruneBelowHeight 清理高度小于该值的区块记录，<=0 不按高度清理。
	PruneBelowHeight uint64
	// AutoPrune 为 true 时每次追加后自动套用保留策略。
	AutoPrune bool
}

// auditTrailFile 是留痕的磁盘格式。
type auditTrailFile struct {
	NextEventID uint64           `json:"nextEventId"`
	Events      []AbandonedEvent `json:"events"`
}

// recordLoc 定位某个区块记录：第 EventIdx 个事件的第 RecordIdx 条记录。
type recordLoc struct {
	eventIdx  int
	recordIdx int
}

// AuditTrail 是独立于主索引的重组留痕存储，所有方法并发安全。
type AuditTrail struct {
	mu          sync.Mutex
	dir         string
	policy      AuditPolicy
	nextEventID uint64
	events      []AbandonedEvent // 按 EventID 升序
	byHeight    map[uint64][]recordLoc
	byTx        map[string]recordLoc
}

// OpenAuditTrail 打开（必要时创建）位于 dir 的留痕存储并恢复历史记录。
func OpenAuditTrail(dir string, policy AuditPolicy) (*AuditTrail, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	t := &AuditTrail{
		dir:      dir,
		policy:   policy,
		byHeight: make(map[uint64][]recordLoc),
		byTx:     make(map[string]recordLoc),
	}
	data, err := os.ReadFile(t.path())
	if os.IsNotExist(err) {
		t.nextEventID = 1
		return t, nil
	}
	if err != nil {
		return nil, err
	}
	var f auditTrailFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("留痕文件损坏: %w", err)
	}
	t.nextEventID = f.NextEventID
	if t.nextEventID == 0 {
		t.nextEventID = 1
	}
	t.events = f.Events
	t.rebuildIndexLocked()
	return t, nil
}

func (t *AuditTrail) path() string { return filepath.Join(t.dir, "audit-trail.json") }

// rebuildIndexLocked 重建高度/交易反查索引；同一交易若在多个被放弃区块中
// 出现（理论上不会），保留事件序号最大的一次定位。
func (t *AuditTrail) rebuildIndexLocked() {
	t.byHeight = make(map[uint64][]recordLoc)
	t.byTx = make(map[string]recordLoc)
	for ei := range t.events {
		for ri, rec := range t.events[ei].Records {
			loc := recordLoc{eventIdx: ei, recordIdx: ri}
			t.byHeight[rec.Height] = append(t.byHeight[rec.Height], loc)
			for _, txh := range rec.TxHashes {
				if old, ok := t.byTx[txh]; !ok || t.events[old.eventIdx].EventID < t.events[ei].EventID {
					t.byTx[txh] = loc
				}
			}
		}
	}
}

// persistLocked 原子写留痕文件（先写临时文件再 rename），调用方须持锁。
func (t *AuditTrail) persistLocked() error {
	f := auditTrailFile{NextEventID: t.nextEventID, Events: t.events}
	data, err := json.Marshal(&f)
	if err != nil {
		return err
	}
	tmp := t.path() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, t.path())
}

// AppendEvent 追加一次重组放弃事件。EventID 为 0 时自动分配；
// BranchID 为空时用被放弃链尖区块标识确定性生成。
// policy.AutoPrune 为 true 时追加后自动套用保留策略。
func (t *AuditTrail) AppendEvent(ev AbandonedEvent) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(ev.Records) == 0 {
		return nil
	}
	if ev.EventID == 0 {
		ev.EventID = t.nextEventID
	}
	if ev.BranchID == "" {
		// 以事件内最高高度区块标识代表被放弃的链尖。
		tip := ev.Records[t.recordIndexOfTip(ev.Records)].BlockHash
		ev.BranchID = "abandoned-" + hashOf("branch", tip)[:12]
	}
	if ev.AbandonedAt == 0 {
		ev.AbandonedAt = time.Now().UnixNano()
	}
	// 记录按高度升序存放，便于阅读与清理。
	sort.Slice(ev.Records, func(i, j int) bool { return ev.Records[i].Height < ev.Records[j].Height })
	t.events = append(t.events, ev)
	if ev.EventID >= t.nextEventID {
		t.nextEventID = ev.EventID + 1
	}
	t.rebuildIndexLocked()
	if t.policy.AutoPrune {
		if _, err := t.pruneLocked(t.policy); err != nil {
			return err
		}
	}
	return t.persistLocked()
}

// recordIndexOfTip 返回 records 中最高高度的下标。
func (t *AuditTrail) recordIndexOfTip(records []AbandonedRecord) int {
	idx := 0
	for i := range records {
		if records[i].Height > records[idx].Height {
			idx = i
		}
	}
	return idx
}

// viewLocked 由定位构造只读视图副本。
func (t *AuditTrail) viewLocked(loc recordLoc) AbandonedRecordView {
	ev := t.events[loc.eventIdx]
	rec := ev.Records[loc.recordIdx]
	txs := append([]string(nil), rec.TxHashes...)
	return AbandonedRecordView{
		EventID:     ev.EventID,
		BranchID:    ev.BranchID,
		Reason:      ev.Reason,
		AbandonedAt: ev.AbandonedAt,
		Height:      rec.Height,
		BlockHash:   rec.BlockHash,
		ParentHash:  rec.ParentHash,
		TxHashes:    txs,
	}
}

// LookupByHeight 按高度反查被放弃的区块留痕（按事件序号升序）。
func (t *AuditTrail) LookupByHeight(height uint64) []AbandonedRecordView {
	t.mu.Lock()
	defer t.mu.Unlock()
	locs := t.byHeight[height]
	out := make([]AbandonedRecordView, 0, len(locs))
	for _, loc := range locs {
		out = append(out, t.viewLocked(loc))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out
}

// LookupByTx 按交易标识反查它曾经所属的被放弃分支。
func (t *AuditTrail) LookupByTx(txHash string) (AbandonedRecordView, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	loc, ok := t.byTx[txHash]
	if !ok {
		return AbandonedRecordView{}, false
	}
	return t.viewLocked(loc), true
}

// Events 返回留痕事件的快照副本（顺序为事件发生先后）。
func (t *AuditTrail) Events() []AbandonedEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]AbandonedEvent, len(t.events))
	for i := range t.events {
		ev := t.events[i]
		ev.Records = append([]AbandonedRecord(nil), ev.Records...)
		out[i] = ev
	}
	return out
}

// Prune 按给定策略清理旧记录，返回被清理的区块记录条数。
// 先按高度清理（PruneBelowHeight），再按条数上限清理（MaxEvents，先进先出）；
// 记录被清空的事件一并删除。只影响留痕，不触碰主索引。
func (t *AuditTrail) Prune(policy AuditPolicy) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pruneLocked(policy)
}

func (t *AuditTrail) pruneLocked(policy AuditPolicy) (int, error) {
	removed := 0
	if policy.PruneBelowHeight > 0 {
		for i := range t.events {
			kept := t.events[i].Records[:0]
			for _, rec := range t.events[i].Records {
				if rec.Height < policy.PruneBelowHeight {
					removed++
					continue
				}
				kept = append(kept, rec)
			}
			t.events[i].Records = kept
		}
	}
	// 删除记录已清空的事件。
	alive := t.events[:0]
	for _, ev := range t.events {
		if len(ev.Records) > 0 {
			alive = append(alive, ev)
		}
	}
	t.events = alive

	if policy.MaxEvents > 0 && len(t.events) > policy.MaxEvents {
		drop := len(t.events) - policy.MaxEvents
		for _, ev := range t.events[:drop] {
			removed += len(ev.Records)
		}
		t.events = append([]AbandonedEvent(nil), t.events[drop:]...)
	}
	t.rebuildIndexLocked()
	if err := t.persistLocked(); err != nil {
		return removed, &Error{Kind: KindInternal, Reason: "留痕持久化失败", Err: err}
	}
	return removed, nil
}

// SetPolicy 更新保留策略；AutoPrune 为 true 时立即套用一次。
func (t *AuditTrail) SetPolicy(policy AuditPolicy) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.policy = policy
	if policy.AutoPrune {
		if _, err := t.pruneLocked(policy); err != nil {
			return err
		}
	}
	return nil
}

// Policy 返回当前保留策略。
func (t *AuditTrail) Policy() AuditPolicy {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.policy
}
