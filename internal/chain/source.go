package chain

import (
	"context"
	"errors"
	"fmt"
)

// DataErrorKind 描述数据源失败的类别。
type DataErrorKind int

const (
	// ErrKindValidation 表示区块/交易字段缺失或格式非法，属于不可恢复失败：
	// 重试无意义，必须显式报告。
	ErrKindValidation DataErrorKind = iota
	// ErrKindTemporary 表示数据源临时失败（如网络抖动、临时不可用），可以安全重试。
	ErrKindTemporary
	// ErrKindFatal 表示不可恢复失败（如数据被声明为永久缺失、链尖视角未知）。
	ErrKindFatal
)

// String 返回类别的可读名称，用于日志与指标区分。
func (k DataErrorKind) String() string {
	switch k {
	case ErrKindValidation:
		return "validation"
	case ErrKindTemporary:
		return "temporary"
	case ErrKindFatal:
		return "fatal"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// DataError 携带可区分的数据源失败原因。
type DataError struct {
	Kind   DataErrorKind
	Height int64
	Reason string
}

func (e *DataError) Error() string {
	return fmt.Sprintf("chain data error kind=%s height=%d: %s", e.Kind, e.Height, e.Reason)
}

// AsDataError 尝试把任意错误解析为 *DataError。
func AsDataError(err error) (*DataError, bool) {
	var de *DataError
	if errors.As(err, &de) {
		return de, true
	}
	return nil, false
}

// DataSource 是区块数据来源抽象。
type DataSource interface {
	// BlockAt 返回指定高度、以 tipHash 为链尖视角的区块。
	// 失败时返回的 error 可被解析为 *DataError 以区分类别。
	BlockAt(ctx context.Context, height int64, tipHash string) (*Block, error)
	// Tip 返回当前链尖区块。
	Tip(ctx context.Context) (*Block, error)
}
