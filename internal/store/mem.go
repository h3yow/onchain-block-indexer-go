package store

import "sync"

// MemStore 是纯内存实现，主要用于测试，也作为 FileStore 的内存核心。
// 所有方法在同一把互斥锁下完成读改写，保证并发摄入下不存在中间破损状态。
type MemStore struct {
	mu sync.Mutex

	blocksByHeight map[int64]BlockRecord
	blocksByHash   map[string]BlockRecord
	txs            map[string]TxRecord // txHash -> 记录（区块内交易哈希唯一）
}

// NewMemStore 创建内存存储。
func NewMemStore() *MemStore {
	return &MemStore{
		blocksByHeight: make(map[int64]BlockRecord),
		blocksByHash:   make(map[string]BlockRecord),
		txs:            make(map[string]TxRecord),
	}
}

// Head 返回最高高度的已索引区块。
func (s *MemStore) Head() (BlockRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var head BlockRecord
	found := false
	for _, b := range s.blocksByHeight {
		if !found || b.Height > head.Height {
			head = b
			found = true
		}
	}
	if !found {
		return BlockRecord{}, ErrNotFound
	}
	return head, nil
}

// BlockAt 按高度返回区块记录。
func (s *MemStore) BlockAt(height int64) (BlockRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocksByHeight[height]
	if !ok {
		return BlockRecord{}, ErrNotFound
	}
	return b, nil
}

// BlockByHash 按哈希返回区块记录。
func (s *MemStore) BlockByHash(hash string) (BlockRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocksByHash[hash]
	if !ok {
		return BlockRecord{}, ErrNotFound
	}
	return b, nil
}

// Tx 按交易哈希返回交易记录。
func (s *MemStore) Tx(txHash string) (TxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.txs[txHash]
	if !ok {
		return TxRecord{}, ErrNotFound
	}
	return t, nil
}

// TxsAt 返回某高度区块内的全部交易（按区块内序号排序）。测试与回滚校验使用。
func (s *MemStore) TxsAt(height int64) ([]TxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.blocksByHeight[height]; !ok {
		return nil, ErrNotFound
	}
	var out []TxRecord
	for _, t := range s.txs {
		if t.Height == height {
			out = append(out, t)
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].BlockOrdinal < out[i].BlockOrdinal {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

// ApplyBlock 在同一原子操作内写入区块与交易，并截断 height 以上的旧记录。
// 同一高度重复提交同一区块是幂等的；提交不同哈希则视为替换（重组重建），
// 该高度旧区块与旧交易的索引全部清除后再写入，绝不残留也不重复。
func (s *MemStore) ApplyBlock(b BlockRecord, txs []TxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyBlockLocked(b, txs)
}

func (s *MemStore) applyBlockLocked(b BlockRecord, txs []TxRecord) error {
	// 幂等：同一高度同一哈希重复摄入时，不产生重复记录。
	if existing, ok := s.blocksByHeight[b.Height]; ok && existing.Hash == b.Hash {
		return nil
	}
	// 替换语义：删除该高度及以上的全部记录（重建受影响区间）。
	s.truncateFromLocked(b.Height)

	s.blocksByHeight[b.Height] = b
	s.blocksByHash[b.Hash] = b
	for _, t := range txs {
		if _, dup := s.txs[t.TxHash]; dup {
			// 理论上交易哈希在整条链内唯一；出现冲突说明数据损坏或数据源缺陷。
			return &ConflictError{Key: "tx:" + t.TxHash}
		}
		s.txs[t.TxHash] = t
	}
	return nil
}

// TruncateFrom 删除 height 及以上的全部区块与交易记录（回滚原语）。
func (s *MemStore) TruncateFrom(height int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncateFromLocked(height)
	return nil
}

func (s *MemStore) truncateFromLocked(height int64) {
	for h, b := range s.blocksByHeight {
		if h >= height {
			delete(s.blocksByHeight, h)
			delete(s.blocksByHash, b.Hash)
		}
	}
	for hash, t := range s.txs {
		if t.Height >= height {
			delete(s.txs, hash)
		}
	}
}

// Close 释放资源；内存实现无副作用。
func (s *MemStore) Close() error { return nil }
