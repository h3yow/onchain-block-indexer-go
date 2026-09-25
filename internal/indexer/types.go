// Package indexer 提供本地可回滚的链上区块与交易索引流水线。
package indexer

import "fmt"

// Tx 是一笔待索引的交易。
type Tx struct {
	Hash   string // 交易唯一标识
	From   string
	To     string
	Amount uint64
}

// Block 是一个待索引的区块。
type Block struct {
	Height     uint64
	Hash       string
	ParentHash string
	Txs        []Tx
}

// IndexedTx 是写入索引后的交易，附带所属区块定位信息。
type IndexedTx struct {
	Tx
	BlockHeight uint64
	BlockHash   string
}

// ErrorKind 对失败原因做可区分的分类。
type ErrorKind int

const (
	// KindInvalidData 区块/交易字段缺失或格式非法（不可重试的数据错误）。
	KindInvalidData ErrorKind = iota
	// KindTransientSource 数据源临时失败（可重试）。
	KindTransientSource
	// KindFatalSource 数据源不可恢复失败（不应重试）。
	KindFatalSource
	// KindReorgTooDeep 重组深度超过配置上限（明确拒绝）。
	KindReorgTooDeep
	// KindInternal 索引器内部错误。
	KindInternal
)

func (k ErrorKind) String() string {
	switch k {
	case KindInvalidData:
		return "invalid-data"
	case KindTransientSource:
		return "transient-source"
	case KindFatalSource:
		return "fatal-source"
	case KindReorgTooDeep:
		return "reorg-too-deep"
	default:
		return "internal"
	}
}

// Error 是带分类、高度与区块标识的流水线错误，便于判定与审计。
type Error struct {
	Kind      ErrorKind
	Height    uint64
	BlockHash string
	Reason    string
	Err       error
}

func (e *Error) Error() string {
	return fmt.Sprintf("[%s] height=%d block=%s: %s", e.Kind, e.Height, e.BlockHash, e.Reason)
}

func (e *Error) Unwrap() error { return e.Err }

// KindOf 从任意 error 中提取分类；非 *Error 一律视为内部错误。
func KindOf(err error) ErrorKind {
	if e, ok := err.(*Error); ok {
		return e.Kind
	}
	return KindInternal
}
