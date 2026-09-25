package indexer

import "sync/atomic"

// Metrics 暴露索引器关键运行指标。所有字段均以原子方式更新，
// 可在摄入进行中被安全读取。
type Metrics struct {
	BlocksIndexed   atomic.Int64 // 成功新写入的区块数（重复摄入不计入）
	TxsIndexed      atomic.Int64 // 成功新写入的交易数
	Rollbacks       atomic.Int64 // 回滚删除的区块数
	Reorgs          atomic.Int64 // 完成处理的重组次数
	RejectedReorgs  atomic.Int64 // 因超过 MaxReorgDepth 被拒绝的重组次数
	TemporaryErrors atomic.Int64 // 数据源临时失败次数
	FatalErrors     atomic.Int64 // 不可恢复失败次数（含字段校验失败）
	DuplicateIngest atomic.Int64 // 幂等重复摄入次数
}

// Snapshot 是某一时刻的指标快照。
type Snapshot struct {
	BlocksIndexed   int64
	TxsIndexed      int64
	Rollbacks       int64
	Reorgs          int64
	RejectedReorgs  int64
	TemporaryErrors int64
	FatalErrors     int64
	DuplicateIngest int64
}

func (m *Metrics) snapshot() Snapshot {
	return Snapshot{
		BlocksIndexed:   m.BlocksIndexed.Load(),
		TxsIndexed:      m.TxsIndexed.Load(),
		Rollbacks:       m.Rollbacks.Load(),
		Reorgs:          m.Reorgs.Load(),
		RejectedReorgs:  m.RejectedReorgs.Load(),
		TemporaryErrors: m.TemporaryErrors.Load(),
		FatalErrors:     m.FatalErrors.Load(),
		DuplicateIngest: m.DuplicateIngest.Load(),
	}
}
