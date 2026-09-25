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
	TipHeight uint64            `json:"tipHeight"`
	TipHash   string            `json:"tipHash"`
	HasTip    bool              `json:"hasTip"`
	Blocks    map[uint64]Block  `json:"blocks"`
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
		b := s.blocksByHei[h]
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
