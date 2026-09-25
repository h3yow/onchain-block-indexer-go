package store

import "errors"

// 存储层错误。
var (
	ErrNotFound = errors.New("store: not found")
)

// BlockRecord / TxRecord 是持久化的索引记录。
type BlockRecord struct {
	Height     int64
	Hash       string
	ParentHash string
}

type TxRecord struct {
	TxHash       string
	BlockHash    string
	Height       int64
	From         string
	To           string
	Value        int64
	BlockOrdinal int // 交易在区块内的序号
}

// Store 是索引持久化抽象。
type Store interface {
	Head() (BlockRecord, error)
	BlockAt(height int64) (BlockRecord, error)
	BlockByHash(hash string) (BlockRecord, error)
	Tx(txHash string) (TxRecord, error)
	// ApplyBlock 在同一原子操作内写入区块与交易，并截断 height 以上的旧记录。
	ApplyBlock(b BlockRecord, txs []TxRecord) error
	// TruncateFrom 删除 height 及以上的全部区块与交易记录。
	TruncateFrom(height int64) error
	Close() error
}

// ConflictError 表示写入时发现唯一键冲突（如交易哈希已属于其它区块）。
type ConflictError struct {
	Key string
}

func (e *ConflictError) Error() string { return "store: unique key conflict on " + e.Key }
