// Package indexer 中的 audit.go 提供重组留痕（旁路审计）：
// 每次重组把被放弃的高度与区块标识持久化下来，重启可查，
// 只做旁路信息，不影响按高度/按交易的常规查询结论。
package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// AuditConfig 控制重组留痕的容量与清理规则。
type AuditConfig struct {
	// Enabled 为 false 时不记录留痕（OpenAuditLog 返回 nil）。
	Enabled bool
	// MaxEntries 为留痕条数上限，<=0 表示不限条数；超出时清理最旧记录。
	MaxEntries int
	// RetainBlocks 为按高度保留的窗口（相对已知最新链尖往回的高度数），
	// <=0 表示不限高度；窗口之外的旧记录允许清理。
	RetainBlocks uint64
}

// AuditEntry 是一次重组中单个被放弃区块的留痕记录。
type AuditEntry struct {
	// ID 为留痕内单调编号（从 1 开始）。
	ID uint64 `json:"id"`
	// ReorgSeq 为该次重组的序号（同一次重组的多个区块共享）。
	ReorgSeq uint64 `json:"reorgSeq"`
	// AbandonedAt 为记录写入时间（Unix 秒）。
	AbandonedAt int64 `json:"abandonedAt"`
	// Height 为被放弃区块的高度。
	Height uint64 `json:"height"`
	// BlockHash 为被放弃区块标识。
	BlockHash string `json:"blockHash"`
	// ParentHash 为被放弃区块的父哈希。
	ParentHash string `json:"parentHash"`
	// TxHashes 为被放弃区块内的全部交易标识。
	TxHashes []string `json:"txHashes"`
	// BranchID 为该区块所属被放弃分支的稳定标识；同一被放弃分支恒定。
	BranchID string `json:"branchId"`
	// ReorgBase 为本次重组的共同祖先高度（回滚点）。
	ReorgBase uint64 `json:"reorgBase"`
	// NewTip 为重组完成后的新链尖高度。
	NewTip uint64 `json:"newTip"`
}

// auditFile 是留痕的磁盘格式。
type auditFile struct {
	NextID    uint64       `json:"nextId"`
	ReorgSeq  uint64       `json:"reorgSeq"`
	LatestTip uint64       `json:"latestTip"`
	Entries   []AuditEntry `json:"entries"`
}

// AuditLog 是持久化的重组留痕日志，所有方法并发安全。
// 它是纯旁路：不参与 BlockAt/TxByHash 的判定，被放弃区块不会借此复活。
type AuditLog struct {
	mu        sync.Mutex
	dir       string
	cfg       AuditConfig
	nextID    uint64
	reorgSeq  uint64
	latestTip uint64
	entries   []AuditEntry
}

// OpenAuditLog 打开（必要时创建）留痕日志并从磁盘恢复；cfg.Enabled=false 时返回 nil。
// 留痕使用独立文件 audit-log.json，与索引检查点互不影响。
func OpenAuditLog(dir string, cfg AuditConfig) (*AuditLog, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	a := &AuditLog{dir: dir, cfg: cfg, nextID: 1}
	data, err := os.ReadFile(a.path())
	if os.IsNotExist(err) {
		return a, nil
	}
	if err != nil {
		return nil, err
	}
	var f auditFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, &Error{Kind: KindInternal, Reason: "留痕文件损坏", Err: err}
	}
	a.nextID, a.reorgSeq, a.latestTip, a.entries = f.NextID, f.ReorgSeq, f.LatestTip, f.Entries
	return a, nil
}

func (a *AuditLog) path() string { return filepath.Join(a.dir, "audit-log.json") }

// persist 原子写留痕文件（先临时文件再 rename），调用方须持锁。
func (a *AuditLog) persist() error {
	f := auditFile{NextID: a.nextID, ReorgSeq: a.reorgSeq, LatestTip: a.latestTip, Entries: a.entries}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	tmp := a.path() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, a.path())
}

// branchIDFor 依据被放弃分支上最低区块（共同祖先的下一个块）派生稳定分支标识。
func branchIDFor(lowest Block) string {
	h := sha256.New()
	h.Write([]byte("abandoned-branch:"))
	h.Write([]byte(lowest.Hash))
	sum := hex.EncodeToString(h.Sum(nil))
	return "br_" + sum[:12]
}

// Append 记录一次重组放弃的区块（顺序不限，内部按高度排序），返回写入的留痕条数。
// 写入后立即按配置清理超限旧记录；清理与写入在同一把锁、同一次原子持久化内完成。
func (a *AuditLog) Append(abandoned []Block, reorgBase, newTip uint64) (int, error) {
	if a == nil || len(abandoned) == 0 {
		return 0, nil
	}
	sorted := make([]Block, len(abandoned))
	copy(sorted, abandoned)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Height < sorted[j].Height })

	a.mu.Lock()
	defer a.mu.Unlock()
	a.reorgSeq++
	if newTip > a.latestTip {
		a.latestTip = newTip
	}
	now := time.Now().Unix()
	branch := branchIDFor(sorted[0])
	added := 0
	for _, b := range sorted {
		txHashes := make([]string, 0, len(b.Txs))
		for _, tx := range b.Txs {
			txHashes = append(txHashes, tx.Hash)
		}
		e := AuditEntry{
			ID:          a.nextID,
			ReorgSeq:    a.reorgSeq,
			AbandonedAt: now,
			Height:      b.Height,
			BlockHash:   b.Hash,
			ParentHash:  b.ParentHash,
			TxHashes:    txHashes,
			BranchID:    branch,
			ReorgBase:   reorgBase,
			NewTip:      newTip,
		}
		a.nextID++
		a.entries = append(a.entries, e)
		added++
	}
	if err := a.applyRetentionLocked(); err != nil {
		return added, err
	}
	if err := a.persist(); err != nil {
		return added, &Error{Kind: KindInternal, Reason: "留痕持久化失败", Err: err}
	}
	return added, nil
}

// applyRetentionLocked 按高度窗口与条数上限裁剪，调用方须持锁。
func (a *AuditLog) applyRetentionLocked() error {
	kept := a.entries[:0]
	var floor uint64
	if a.cfg.RetainBlocks > 0 && a.latestTip > a.cfg.RetainBlocks {
		floor = a.latestTip - a.cfg.RetainBlocks + 1
	}
	for _, e := range a.entries {
		if floor > 0 && e.Height < floor {
			continue
		}
		kept = append(kept, e)
	}
	a.entries = kept
	if a.cfg.MaxEntries > 0 && len(a.entries) > a.cfg.MaxEntries {
		a.entries = a.entries[len(a.entries)-a.cfg.MaxEntries:]
	}
	return nil
}

// Prune 按配置的条数/高度窗口清理旧留痕，返回删除条数；只动留痕文件，
// 绝不触碰索引检查点。
func (a *AuditLog) Prune() (int, error) {
	if a == nil {
		return 0, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	before := len(a.entries)
	if err := a.applyRetentionLocked(); err != nil {
		return 0, err
	}
	removed := before - len(a.entries)
	if removed == 0 {
		return 0, nil
	}
	if err := a.persist(); err != nil {
		return 0, &Error{Kind: KindInternal, Reason: "留痕清理持久化失败", Err: err}
	}
	return removed, nil
}

func (a *AuditLog) clone() []AuditEntry {
	out := make([]AuditEntry, len(a.entries))
	copy(out, a.entries)
	return out
}

// All 返回全部留痕，按编号升序。
func (a *AuditLog) All() []AuditEntry {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.clone()
}

// ByHeight 返回指定高度的全部留痕（可能跨越多次重组/多个被放弃分支）。
func (a *AuditLog) ByHeight(height uint64) []AuditEntry {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []AuditEntry
	for _, e := range a.entries {
		if e.Height == height {
			out = append(out, e)
		}
	}
	return out
}

// ByBlock 返回指定区块标识的留痕。
func (a *AuditLog) ByBlock(blockHash string) []AuditEntry {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []AuditEntry
	for _, e := range a.entries {
		if e.BlockHash == blockHash {
			out = append(out, e)
		}
	}
	return out
}

// ByTx 返回包含指定交易标识的留痕，可据此反查交易曾属于哪个被放弃分支。
func (a *AuditLog) ByTx(txHash string) []AuditEntry {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []AuditEntry
	for _, e := range a.entries {
		for _, h := range e.TxHashes {
			if h == txHash {
				out = append(out, e)
				break
			}
		}
	}
	return out
}

// Len 返回留痕条数。
func (a *AuditLog) Len() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.entries)
}

// Config 返回留痕配置（只读用途）。
func (a *AuditLog) Config() AuditConfig {
	if a == nil {
		return AuditConfig{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg
}
