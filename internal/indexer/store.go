package indexer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// validateBlock 校验区块与交易字段，缺失或非法时返回 KindInvalidData，
// 绝不静默丢弃。
func validateBlock(b Block) error {
	fail := func(reason string) *Error {
		return &Error{Kind: KindInvalidData, Height: b.Height, BlockHash: b.Hash, Reason: reason}
	}
	if b.Height == 0 {
		return fail("height 缺失（必须 >= 1）")
	}
	if b.Hash == "" {
		return fail("block hash 缺失")
	}
	if b.ParentHash == "" {
		return fail("parent hash 缺失")
	}
	seen := make(map[string]bool, len(b.Txs))
	for i, tx := range b.Txs {
		if tx.Hash == "" {
			return fail(fmt.Sprintf("tx[%d] hash 缺失", i))
		}
		if tx.From == "" || tx.To == "" {
			return fail(fmt.Sprintf("tx[%d](%s) from/to 缺失", i, tx.Hash))
		}
		if seen[tx.Hash] {
			return fail(fmt.Sprintf("tx[%d](%s) 在区块内重复", i, tx.Hash))
		}
		seen[tx.Hash] = true
	}
	return nil
}

// checkpoint 是持久化到磁盘的索引状态。
type checkpoint struct {
	TipHeight uint64               `json:"tipHeight"`
	TipHash   string               `json:"tipHash"`
	HasTip    bool                 `json:"hasTip"`
	Blocks    map[uint64]Block     `json:"blocks"`
	Txs       map[string]IndexedTx `json:"txs"`
}

// Store 是带持久化检查点的索引存储，所有方法并发安全。
type Store struct {
	mu            sync.RWMutex
	dir           string
	blocksByHei   map[uint64]Block
	blockHeiByHsh map[string]uint64
	txs           map[string]IndexedTx
	tipHeight     uint64
	tipHash       string
	hasTip        bool
}

// OpenStore 打开（必要时创建）位于 dir 的存储，并从检查点恢复进度。
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:           dir,
		blocksByHei:   make(map[uint64]Block),
		blockHeiByHsh: make(map[string]uint64),
		txs:           make(map[string]IndexedTx),
	}
	data, err := os.ReadFile(s.ckptPath())
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var ck checkpoint
	if err := json.Unmarshal(data, &ck); err != nil {
		return nil, fmt.Errorf("检查点损坏: %w", err)
	}
	s.tipHeight, s.tipHash, s.hasTip = ck.TipHeight, ck.TipHash, ck.HasTip
	for h, b := range ck.Blocks {
		s.blocksByHei[h] = b
		s.blockHeiByHsh[b.Hash] = h
	}
	for h, t := range ck.Txs {
		s.txs[h] = t
	}
	return s, nil
}

func (s *Store) ckptPath() string { return filepath.Join(s.dir, "checkpoint.json") }

// persist 原子写检查点（先写临时文件再 rename），调用方须持写锁。
func (s *Store) persist() error {
	ck := checkpoint{
		TipHeight: s.tipHeight,
		TipHash:   s.tipHash,
		HasTip:    s.hasTip,
		Blocks:    s.blocksByHei,
		Txs:       s.txs,
	}
	data, err := json.Marshal(ck)
	if err != nil {
		return err
	}
	tmp := s.ckptPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.ckptPath())
}

// Tip 返回当前索引链尖；无区块时 ok=false。
func (s *Store) Tip() (height uint64, hash string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tipHeight, s.tipHash, s.hasTip
}

// ApplyBlock 幂等地写入一个区块及其交易，并推进链尖。
// 返回值 applied=false 表示同高度同哈希的重复摄入（幂等跳过）。
// 同高度不同哈希属于未回滚的冲突，返回错误，调用方须先回滚。
func (s *Store) ApplyBlock(b Block) (applied bool, err error) {
	if err := validateBlock(b); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.blocksByHei[b.Height]; ok {
		if old.Hash == b.Hash {
			return false, nil // 幂等：重复摄入不产生重复记录
		}
		return false, &Error{
			Kind: KindInternal, Height: b.Height, BlockHash: b.Hash,
			Reason: fmt.Sprintf("高度已被不同区块 %s 占用，需先回滚", old.Hash),
		}
	}
	if s.hasTip && b.Height != s.tipHeight+1 {
		return false, &Error{
			Kind: KindInternal, Height: b.Height, BlockHash: b.Hash,
			Reason: fmt.Sprintf("高度不连续：当前链尖 %d", s.tipHeight),
		}
	}
	if s.hasTip && b.ParentHash != s.tipHash {
		return false, &Error{
			Kind: KindInternal, Height: b.Height, BlockHash: b.Hash,
			Reason: fmt.Sprintf("parent %s 与链尖 %s 不一致，需先重组回滚", b.ParentHash, s.tipHash),
		}
	}
	s.blocksByHei[b.Height] = b
	s.blockHeiByHsh[b.Hash] = b.Height
	for _, tx := range b.Txs {
		s.txs[tx.Hash] = IndexedTx{Tx: tx, BlockHeight: b.Height, BlockHash: b.Hash}
	}
	s.tipHeight, s.tipHash, s.hasTip = b.Height, b.Hash, true
	if err := s.persist(); err != nil {
		return false, &Error{Kind: KindInternal, Height: b.Height, BlockHash: b.Hash, Reason: "检查点持久化失败", Err: err}
	}
	return true, nil
}

// RollbackTo 回滚到指定高度（含），删除其上的所有区块与交易。
// 返回被回滚的区块数。
func (s *Store) RollbackTo(height uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasTip {
		return 0, nil
	}
	target, ok := s.blocksByHei[height]
	if height == 0 {
		// 全量回滚到创世之前。
		ok = true
		target = Block{}
	}
	if !ok {
		return 0, &Error{
			Kind: KindInternal, Height: height,
			Reason: fmt.Sprintf("回滚目标高度 %d 不在索引中（链尖 %d）", height, s.tipHeight),
		}
	}
	reverted := 0
	for h := s.tipHeight; h > height; h-- {
		b, ok := s.blocksByHei[h]
		if !ok {
			continue // 区间内本就缺失的高度不计入回滚数
		}
		for _, tx := range b.Txs {
			delete(s.txs, tx.Hash)
		}
		delete(s.blockHeiByHsh, b.Hash)
		delete(s.blocksByHei, h)
		reverted++
	}
	s.tipHeight, s.tipHash = target.Height, target.Hash
	s.hasTip = height != 0
	if err := s.persist(); err != nil {
		return reverted, &Error{Kind: KindInternal, Height: height, Reason: "检查点持久化失败", Err: err}
	}
	return reverted, nil
}

// Snapshot 是某一时刻已提交索引的不可变只读副本。
// 快照在单个读锁内拷贝完成，巡检据此对账，绝不与摄入的中间状态交错。
type Snapshot struct {
	TipHeight uint64
	TipHash   string
	HasTip    bool
	Blocks    map[uint64]Block
	Txs       map[string]IndexedTx
}

// Snapshot 取出当前已提交状态的一致快照（只读拷贝）。
func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{
		TipHeight: s.tipHeight,
		TipHash:   s.tipHash,
		HasTip:    s.hasTip,
		Blocks:    make(map[uint64]Block, len(s.blocksByHei)),
		Txs:       make(map[string]IndexedTx, len(s.txs)),
	}
	for h, b := range s.blocksByHei {
		snap.Blocks[h] = b
	}
	for h, t := range s.txs {
		snap.Txs[h] = t
	}
	return snap
}

// PurgeAbove 删除高度严格大于 keepTip 的全部区块与交易，并把链尖落到
// keepTip（keepTip 必须存在于索引或为 0）。用于自愈时清理索引中高于
// 数据源链尖的多余数据，返回删除的区块数。调用方须已确认这是巡检结论。
func (s *Store) PurgeAbove(keepTip uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if keepTip > 0 {
		if _, ok := s.blocksByHei[keepTip]; !ok {
			return 0, &Error{
				Kind: KindInternal, Height: keepTip,
				Reason: fmt.Sprintf("清理目标高度 %d 不在索引中（链尖 %d）", keepTip, s.tipHeight),
			}
		}
	}
	purged := 0
	for h, b := range s.blocksByHei {
		if h <= keepTip {
			continue
		}
		for _, tx := range b.Txs {
			delete(s.txs, tx.Hash)
		}
		delete(s.blockHeiByHsh, b.Hash)
		delete(s.blocksByHei, h)
		purged++
	}
	if keepTip == 0 {
		s.tipHeight, s.tipHash, s.hasTip = 0, "", false
	} else {
		target := s.blocksByHei[keepTip]
		s.tipHeight, s.tipHash, s.hasTip = keepTip, target.Hash, true
	}
	if err := s.persist(); err != nil {
		return purged, &Error{Kind: KindInternal, Height: keepTip, Reason: "检查点持久化失败", Err: err}
	}
	return purged, nil
}

// PurgeDanglingTxs 删除不属于任何已提交区块的交易，返回删除条数。
// 只按区块内容做归属判定，不改动区块与链尖。
func (s *Store) PurgeDanglingTxs() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	owned := make(map[string]bool, len(s.txs))
	for _, b := range s.blocksByHei {
		for _, tx := range b.Txs {
			owned[tx.Hash] = true
		}
	}
	purged := 0
	for h := range s.txs {
		if !owned[h] {
			delete(s.txs, h)
			purged++
		}
	}
	if purged == 0 {
		return 0, nil
	}
	if err := s.persist(); err != nil {
		return purged, &Error{Kind: KindInternal, Reason: "检查点持久化失败", Err: err}
	}
	return purged, nil
}

// BlockAt 按高度查询已提交区块。
func (s *Store) BlockAt(height uint64) (Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocksByHei[height]
	return b, ok
}

// TxByHash 按交易标识查询已提交交易。
func (s *Store) TxByHash(hash string) (IndexedTx, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.txs[hash]
	return t, ok
}
