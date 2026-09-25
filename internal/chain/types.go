package chain

import (
	"fmt"
	"regexp"
)

// Block 表示链上的一个区块（由本地模拟数据源生成）。
type Block struct {
	Height       int64
	Hash         string
	ParentHash   string
	Transactions []Transaction
}

// Transaction 表示区块中的一笔交易。
type Transaction struct {
	Hash string
	From string
	To   string
	// Value 使用最小货币单位的整数表示，避免浮点误差。
	Value int64
}

// GenesisHash 是创世块（高度 0）的约定哈希。
const GenesisHash = "genesis"

var (
	hashRe    = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
	addressRe = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
)

// ValidateBlock 校验区块及其交易字段，缺失或格式非法时返回 ErrKindValidation
// 类别的 DataError，错误原因可区分，绝不静默丢弃。
func ValidateBlock(b *Block) error {
	if b == nil {
		return &DataError{Kind: ErrKindValidation, Height: -1, Reason: "block is nil"}
	}
	if b.Height < 0 {
		return &DataError{Kind: ErrKindValidation, Height: b.Height, Reason: "block height is negative"}
	}
	if b.Hash == "" {
		return &DataError{Kind: ErrKindValidation, Height: b.Height, Reason: "block hash is empty"}
	}
	if b.Height == 0 {
		if b.ParentHash != "" {
			return &DataError{Kind: ErrKindValidation, Height: b.Height,
				Reason: fmt.Sprintf("genesis block parent hash must be empty, got %q", b.ParentHash)}
		}
	} else if b.ParentHash == "" {
		return &DataError{Kind: ErrKindValidation, Height: b.Height, Reason: "non-genesis block parent hash is empty"}
	}
	seen := make(map[string]struct{}, len(b.Transactions))
	for i, tx := range b.Transactions {
		if err := ValidateTx(b.Height, i, tx); err != nil {
			return err
		}
		if _, dup := seen[tx.Hash]; dup {
			return &DataError{Kind: ErrKindValidation, Height: b.Height,
				Reason: fmt.Sprintf("duplicate transaction hash %s within block", tx.Hash)}
		}
		seen[tx.Hash] = struct{}{}
	}
	return nil
}

// ValidateTx 校验单笔交易字段。
func ValidateTx(blockHeight int64, ordinal int, tx Transaction) error {
	if !hashRe.MatchString(tx.Hash) {
		return &DataError{Kind: ErrKindValidation, Height: blockHeight,
			Reason: fmt.Sprintf("tx at ordinal %d has invalid hash %q: want 0x + 64 hex digits", ordinal, tx.Hash)}
	}
	if !addressRe.MatchString(tx.From) {
		return &DataError{Kind: ErrKindValidation, Height: blockHeight,
			Reason: fmt.Sprintf("tx %s has invalid from address %q: want 0x + 40 hex digits", tx.Hash, tx.From)}
	}
	if !addressRe.MatchString(tx.To) {
		return &DataError{Kind: ErrKindValidation, Height: blockHeight,
			Reason: fmt.Sprintf("tx %s has invalid to address %q: want 0x + 40 hex digits", tx.Hash, tx.To)}
	}
	if tx.Value < 0 {
		return &DataError{Kind: ErrKindValidation, Height: blockHeight,
			Reason: fmt.Sprintf("tx %s has negative value %d", tx.Hash, tx.Value)}
	}
	return nil
}
