package store

import (
	"path/filepath"
	"testing"
)

func br(h int64, hash, parent string) BlockRecord {
	return BlockRecord{Height: h, Hash: hash, ParentHash: parent}
}

func tx(hash string, height int64, ordinal int) TxRecord {
	return TxRecord{TxHash: hash, BlockHash: "", Height: height, BlockOrdinal: ordinal}
}

func TestMemStoreApplyIdempotent(t *testing.T) {
	st := NewMemStore()
	b := br(1, "0xb1", "genesis")
	txs := []TxRecord{{TxHash: "0xt1", BlockHash: "0xb1", Height: 1, BlockOrdinal: 0}}

	if err := st.ApplyBlock(b, txs); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	// 同高度同哈希重复提交：幂等，无重复记录。
	if err := st.ApplyBlock(b, txs); err != nil {
		t.Fatalf("idempotent apply: %v", err)
	}
	got, err := st.Tx("0xt1")
	if err != nil {
		t.Fatalf("tx lookup: %v", err)
	}
	if got.Height != 1 {
		t.Fatalf("tx height=%d want 1", got.Height)
	}
	head, _ := st.Head()
	t.Logf("after duplicate apply: head height=%d block=%s basis=same hash is idempotent", head.Height, head.Hash)
}

func TestMemStoreReplaceAndRollbackLeavesNoResidue(t *testing.T) {
	st := NewMemStore()
	for h := int64(0); h <= 4; h++ {
		hash := "0xold" + string(rune('0'+h))
		parent := ""
		if h > 0 {
			parent = "0xold" + string(rune('0'+h-1))
		}
		txs := []TxRecord{{TxHash: "0xoldtx" + string(rune('0'+h)), BlockHash: hash, Height: h, BlockOrdinal: 0}}
		if err := st.ApplyBlock(br(h, hash, parent), txs); err != nil {
			t.Fatalf("apply h=%d: %v", h, err)
		}
	}

	// 在高度 3 处用不同哈希替换（模拟重组重建第一块）。
	newHash := "0xnew3"
	newTxs := []TxRecord{{TxHash: "0xnewtx3", BlockHash: newHash, Height: 3, BlockOrdinal: 0}}
	if err := st.ApplyBlock(br(3, newHash, "0xold2"), newTxs); err != nil {
		t.Fatalf("replace apply: %v", err)
	}

	// 高度 4 的旧区块与旧交易必须被清除，不得残留。
	if _, err := st.BlockAt(4); err != ErrNotFound {
		t.Fatalf("abandoned block at 4 must be gone, err=%v", err)
	}
	if _, err := st.Tx("0xoldtx4"); err != ErrNotFound {
		t.Fatalf("abandoned tx at 4 must be gone, err=%v", err)
	}
	// 高度 3 的旧记录也必须被替换干净。
	if _, err := st.BlockByHash("0xold3"); err != ErrNotFound {
		t.Fatalf("old block hash at 3 must be gone, err=%v", err)
	}
	if _, err := st.Tx("0xoldtx3"); err != ErrNotFound {
		t.Fatalf("old tx at 3 must be gone, err=%v", err)
	}
	got, _ := st.Tx("0xnewtx3")
	if got.BlockHash != newHash {
		t.Fatalf("new tx not found, got block hash %q", got.BlockHash)
	}
	t.Logf("reorg residue check passed: height=4 old block/tx removed, height=3 replaced by block=%s", newHash)
}

func TestFileStoreRestartResumesFromSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")

	st, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.ApplyBlock(br(0, "genesis", ""), nil); err != nil {
		t.Fatalf("apply genesis: %v", err)
	}
	if err := st.ApplyBlock(br(1, "0xb1", "genesis"),
		[]TxRecord{{TxHash: "0xt1", BlockHash: "0xb1", Height: 1, BlockOrdinal: 0}}); err != nil {
		t.Fatalf("apply 1: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开：进度恢复，重启后重复 Apply 同一块必须仍然幂等。
	st2, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	head, err := st2.Head()
	if err != nil {
		t.Fatalf("head after restart: %v", err)
	}
	if head.Height != 1 || head.Hash != "0xb1" {
		t.Fatalf("head after restart = %+v, want height=1 hash=0xb1", head)
	}
	if err := st2.ApplyBlock(br(1, "0xb1", "genesis"),
		[]TxRecord{{TxHash: "0xt1", BlockHash: "0xb1", Height: 1, BlockOrdinal: 0}}); err != nil {
		t.Fatalf("idempotent apply after restart: %v", err)
	}
	t.Logf("restart resumed at height=%d block=%s basis=snapshot head", head.Height, head.Hash)
}
