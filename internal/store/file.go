package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// snapshot 是磁盘上的索引状态快照。进度（head）由 blocks 推导，
// 因此重启后天然从已持久化的最高高度继续，不重复、不丢失。
type snapshot struct {
	Blocks []BlockRecord `json:"blocks"`
	Txs    []TxRecord    `json:"txs"`
}

// FileStore 基于 JSON 快照文件的持久化实现。
// 每次成功提交后把全量状态写入同目录临时文件并原子 rename，
// 进程崩溃只会让最后一次未完成提交回退到上一个完整快照。
type FileStore struct {
	*MemStore

	persistMu sync.Mutex // 序列化"改内存 + 落盘"，保证快照与内存一致
	path      string
}

// OpenFileStore 打开快照文件；文件不存在时初始化为空索引。
func OpenFileStore(path string) (*FileStore, error) {
	fs := &FileStore{MemStore: NewMemStore(), path: path}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fs, nil // 全新启动：空索引
		}
		return nil, fmt.Errorf("store: read snapshot %s: %w", path, err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("store: corrupt snapshot %s: %w", path, err)
	}
	for _, b := range snap.Blocks {
		fs.MemStore.blocksByHeight[b.Height] = b
		fs.MemStore.blocksByHash[b.Hash] = b
	}
	for _, t := range snap.Txs {
		fs.MemStore.txs[t.TxHash] = t
	}
	return fs, nil
}

// ApplyBlock 原子地写入区块与交易并落盘。
func (s *FileStore) ApplyBlock(b BlockRecord, txs []TxRecord) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	s.MemStore.mu.Lock()
	if err := s.MemStore.applyBlockLocked(b, txs); err != nil {
		s.MemStore.mu.Unlock()
		return err
	}
	s.MemStore.mu.Unlock()

	return s.persistLocked()
}

// TruncateFrom 删除 height 及以上记录并落盘。
func (s *FileStore) TruncateFrom(height int64) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	s.MemStore.mu.Lock()
	s.MemStore.truncateFromLocked(height)
	s.MemStore.mu.Unlock()

	return s.persistLocked()
}

func (s *FileStore) persistLocked() error {
	s.MemStore.mu.Lock()
	snap := snapshot{
		Blocks: make([]BlockRecord, 0, len(s.MemStore.blocksByHeight)),
		Txs:    make([]TxRecord, 0, len(s.MemStore.txs)),
	}
	for _, b := range s.MemStore.blocksByHeight {
		snap.Blocks = append(snap.Blocks, b)
	}
	for _, t := range s.MemStore.txs {
		snap.Txs = append(snap.Txs, t)
	}
	s.MemStore.mu.Unlock()

	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal snapshot: %w", err)
	}

	dir := filepath.Dir(s.path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".index-snap-*")
	if err != nil {
		return fmt.Errorf("store: create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 一个不存在的文件无副作用

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("store: write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("store: fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("store: close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("store: atomically replace snapshot: %w", err)
	}
	return nil
}
