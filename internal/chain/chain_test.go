package chain

import (
	"context"
	"testing"
)

func validTx(suffix byte) Transaction {
	return Transaction{
		Hash:  "0x" + repeat("1", 63) + string([]byte{suffix}),
		From:  "0x" + repeat("2", 40),
		To:    "0x" + repeat("3", 40),
		Value: 1,
	}
}

func repeat(c string, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c[0]
	}
	return string(b)
}

func TestValidateBlockFailures(t *testing.T) {
	cases := []struct {
		name   string
		block  *Block
		reason string // Reason 中必须包含的可区分片段
	}{
		{
			name:   "nil block",
			block:  nil,
			reason: "block is nil",
		},
		{
			name:   "empty block hash",
			block:  &Block{Height: 1, Hash: "", ParentHash: "0xabc"},
			reason: "block hash is empty",
		},
		{
			name: "missing parent hash",
			block: &Block{Height: 2, Hash: "0xaaa", ParentHash: "",
				Transactions: []Transaction{validTx('1')}},
			reason: "parent hash is empty",
		},
		{
			name:   "genesis with non-empty parent",
			block:  &Block{Height: 0, Hash: "g", ParentHash: "0xaaa"},
			reason: "genesis block parent hash must be empty",
		},
		{
			name: "malformed tx hash",
			block: &Block{Height: 1, Hash: "0xaaa", ParentHash: "genesis",
				Transactions: []Transaction{{Hash: "nope", From: "0x" + repeat("2", 40), To: "0x" + repeat("3", 40)}}},
			reason: "invalid hash",
		},
		{
			name: "missing from address",
			block: &Block{Height: 1, Hash: "0xaaa", ParentHash: "genesis",
				Transactions: []Transaction{func() Transaction {
					tx := validTx('1')
					tx.From = ""
					return tx
				}()}},
			reason: "invalid from address",
		},
		{
			name: "negative value",
			block: &Block{Height: 1, Hash: "0xaaa", ParentHash: "genesis",
				Transactions: []Transaction{func() Transaction {
					tx := validTx('1')
					tx.Value = -5
					return tx
				}()}},
			reason: "negative value",
		},
		{
			name: "duplicate tx within block",
			block: &Block{Height: 1, Hash: "0xaaa", ParentHash: "genesis",
				Transactions: []Transaction{validTx('9'), validTx('9')}},
			reason: "duplicate transaction hash",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBlock(tc.block)
			if err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			de, ok := AsDataError(err)
			if !ok {
				t.Fatalf("error %v is not *DataError", err)
			}
			if de.Kind != ErrKindValidation {
				t.Fatalf("expected ErrKindValidation, got %s", de.Kind)
			}
			if !contains(de.Reason, tc.reason) {
				t.Fatalf("reason %q does not contain %q", de.Reason, tc.reason)
			}
			t.Logf("height=%d classified=%s basis=%s", de.Height, de.Kind, de.Reason)
		})
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestSimSourceFailuresAreClassified(t *testing.T) {
	ctx := context.Background()
	src := NewSimSource()
	src.ExtendN(3)

	// 临时失败：前两次读取失败，第三次成功。
	src.FailTemporary(2, 2)
	if _, err := src.BlockAt(ctx, 2, src.TipHash()); !kindIs(t, err, ErrKindTemporary) {
		t.Fatalf("want temporary, got %v", err)
	}
	if _, err := src.BlockAt(ctx, 2, src.TipHash()); !kindIs(t, err, ErrKindTemporary) {
		t.Fatalf("want temporary, got %v", err)
	}
	b, err := src.BlockAt(ctx, 2, src.TipHash())
	if err != nil {
		t.Fatalf("want recovery after temporary failures, got %v", err)
	}
	t.Logf("recovered block height=%d hash=%s basis=temporary failures exhausted", b.Height, b.Hash)

	// 不可恢复失败。
	src.FailFatal(3)
	src.Extend()
	if _, err := src.BlockAt(ctx, 3, src.TipHash()); !kindIs(t, err, ErrKindFatal) {
		t.Fatalf("want fatal, got %v", err)
	}
	src.ClearFailure(3)

	// 未知链尖视角属于不可恢复。
	if _, err := src.BlockAt(ctx, 1, "0xdead"); !kindIs(t, err, ErrKindFatal) {
		t.Fatalf("want fatal for unknown tip, got %v", err)
	}
}

func kindIs(t *testing.T, err error, kind DataErrorKind) bool {
	t.Helper()
	de, ok := AsDataError(err)
	if !ok {
		t.Errorf("error %v is not *DataError", err)
		return false
	}
	t.Logf("classified error kind=%s height=%d reason=%s", de.Kind, de.Height, de.Reason)
	return de.Kind == kind
}

func TestSimSourceReorgKeepsOldTipView(t *testing.T) {
	ctx := context.Background()
	src := NewSimSource()
	blocks := src.ExtendN(4)

	oldTip := src.TipHash()
	oldH4 := blocks[3].Hash
	newTip := src.Reorg(2, 3)
	if newTip == oldTip {
		t.Fatalf("reorg must produce a new tip hash")
	}

	// 旧链尖视角仍可读到旧区块；新链尖视角读到新区块，二者可区分。
	oldB, err := src.BlockAt(ctx, 4, oldTip)
	if err != nil {
		t.Fatalf("old tip view lost: %v", err)
	}
	if oldB.Hash != oldH4 {
		t.Fatalf("old tip block mismatch: got %s want %s", oldB.Hash, oldH4)
	}
	newB, err := src.BlockAt(ctx, 4, newTip)
	if err != nil {
		t.Fatalf("new tip block: %v", err)
	}
	if newB.Hash == oldH4 {
		t.Fatalf("new tip block must differ from abandoned block")
	}
	// 高度 2 之前是共同祖先。
	prefixB, err := src.BlockAt(ctx, 2, newTip)
	if err != nil {
		t.Fatalf("common prefix: %v", err)
	}
	if prefixB.Hash != blocks[1].Hash {
		t.Fatalf("common ancestor mismatch: got %s want %s", prefixB.Hash, blocks[1].Hash)
	}
	t.Logf("reorg views verified: height=4 oldtip=%s newtip=%s ancestor-height=2 ancestor=%s",
		oldB.Hash, newB.Hash, prefixB.Hash)
}
