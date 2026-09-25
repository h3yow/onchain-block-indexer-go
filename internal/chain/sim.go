package chain

import (
	"context"
	"fmt"
	"sync"
)

// failRule 描述针对某个高度的注入故障。
type failRule struct {
	temporaryLeft int  // 剩余临时失败次数；耗尽后自动恢复
	fatal         bool // 是否为不可恢复失败
}

// SimSource 是本地内存模拟数据源。
//
// 它在内存中维护若干条完整链（从创世块到各自链尖），按链尖哈希索引；
// 发生分叉时旧链保留，BlockAt 始终按调用方给定的 tipHash 视角取块，
// 从而模拟真实链客户端"在某个链尖视角下读历史"的语义。
type SimSource struct {
	mu sync.Mutex

	chains  map[string][]*Block // tipHash -> 从创世到链尖的完整区块序列
	tipHash string              // 当前规范链链尖

	blockSeq int // 区块哈希生成计数（保证跨分叉唯一）
	txSeq    int // 交易哈希生成计数

	rules map[int64]*failRule // 按高度注入的故障
}

// NewSimSource 创建仅含创世块的模拟数据源。
func NewSimSource() *SimSource {
	genesis := &Block{Height: 0, Hash: GenesisHash, ParentHash: "", Transactions: nil}
	s := &SimSource{
		chains:  map[string][]*Block{GenesisHash: {genesis}},
		tipHash: GenesisHash,
		rules:   make(map[int64]*failRule),
	}
	return s
}

// TipHash 返回当前规范链链尖哈希。
func (s *SimSource) TipHash() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tipHash
}

// CurrentHeight 返回当前规范链高度。
func (s *SimSource) CurrentHeight() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.chains[s.tipHash])) - 1
}

func (s *SimSource) nextHash() string {
	s.blockSeq++
	return fmt.Sprintf("0x%064x", s.blockSeq)
}

func (s *SimSource) nextTx() Transaction {
	s.txSeq++
	return Transaction{
		Hash:  fmt.Sprintf("0x%064x", 1000000+s.txSeq),
		From:  fmt.Sprintf("0x%040x", 1000+s.txSeq),
		To:    fmt.Sprintf("0x%040x", 2000+s.txSeq),
		Value: int64(s.txSeq),
	}
}

// Extend 在当前规范链尾部追加一个区块（含一笔确定性唯一交易），返回新区块。
func (s *SimSource) Extend() *Block {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.chains[s.tipHash]
	b := s.makeBlock(int64(len(old)), old[len(old)-1].Hash, []Transaction{s.nextTx()})
	s.appendBlockLocked(b)
	return cloneBlock(b)
}

// ExtendN 连续追加 n 个区块。
func (s *SimSource) ExtendN(n int) []*Block {
	out := make([]*Block, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s.Extend())
	}
	return out
}

// ExtendEmpty 追加一个不含交易的区块（空块是合法的）。
func (s *SimSource) ExtendEmpty() *Block {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.chains[s.tipHash]
	b := s.makeBlock(int64(len(old)), old[len(old)-1].Hash, nil)
	s.appendBlockLocked(b)
	return cloneBlock(b)
}

func (s *SimSource) makeBlock(height int64, parent string, txs []Transaction) *Block {
	return &Block{
		Height:       height,
		Hash:         s.nextHash(),
		ParentHash:   parent,
		Transactions: txs,
	}
}

// appendBlockLocked 把新区块接到当前链尖：复制整条链并以新哈希为键保存，
// 旧链尖视角保持不变。
func (s *SimSource) appendBlockLocked(b *Block) {
	old := s.chains[s.tipHash]
	chain := make([]*Block, 0, len(old)+1)
	chain = append(chain, old...)
	chain = append(chain, b)
	s.tipHash = b.Hash
	s.chains[s.tipHash] = chain
}

// Reorg 在 forkHeight 处制造分叉：保留高度 [0, forkHeight] 的共同前缀，
// 丢弃其后旧区块，追加 newLen 个全新哈希与交易的区块成为新规范链。
// 返回新链尖哈希。
func (s *SimSource) Reorg(forkHeight int64, newLen int) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.chains[s.tipHash]
	if forkHeight < 0 || forkHeight > int64(len(old))-1 {
		panic(fmt.Sprintf("sim: fork height %d out of range [0,%d]", forkHeight, len(old)-1))
	}
	if newLen <= 0 {
		panic("sim: reorg new length must be positive")
	}

	prefix := make([]*Block, forkHeight+1)
	copy(prefix, old[:forkHeight+1])

	chain := prefix
	var tip string
	for i := int64(0); i < int64(newLen); i++ {
		height := forkHeight + 1 + i
		parent := GenesisHash
		if height > 0 {
			parent = chain[height-1].Hash
		}
		b := s.makeBlock(height, parent, []Transaction{s.nextTx()})
		chain = append(chain, b)
		tip = b.Hash
	}
	s.tipHash = tip
	s.chains[tip] = chain
	return tip
}

// OverrideBlock 用调用方构造的区块替换当前链指定高度的区块，
// 用于注入字段缺失/格式非法的数据。父哈希不会被联动修改。
func (s *SimSource) OverrideBlock(height int64, b *Block) {
	s.mu.Lock()
	defer s.mu.Unlock()

	chain := s.chains[s.tipHash]
	if height < 0 || height > int64(len(chain))-1 {
		panic(fmt.Sprintf("sim: override height %d out of range", height))
	}
	chain[height] = b
}

// FailTemporary 令对 height 的接下来 n 次读取返回临时失败，之后自动恢复。
func (s *SimSource) FailTemporary(height int64, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[height] = &failRule{temporaryLeft: n}
}

// FailFatal 令对 height 的读取持续返回不可恢复失败。
func (s *SimSource) FailFatal(height int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[height] = &failRule{fatal: true}
}

// ClearFailure 清除某高度上的注入故障。
func (s *SimSource) ClearFailure(height int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rules, height)
}

// BlockAt 实现 DataSource：按 tipHash 视角返回 height 处区块的深拷贝。
func (s *SimSource) BlockAt(_ context.Context, height int64, tipHash string) (*Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tipHash == "" {
		tipHash = s.tipHash
	}
	chain, ok := s.chains[tipHash]
	if !ok {
		return nil, &DataError{Kind: ErrKindFatal, Height: height,
			Reason: fmt.Sprintf("unknown tip hash %s", tipHash)}
	}
	if height < 0 || height > int64(len(chain))-1 {
		return nil, &DataError{Kind: ErrKindFatal, Height: height,
			Reason: fmt.Sprintf("block at height %d permanently unavailable under tip %s (chain height %d)",
				height, tipHash, len(chain)-1)}
	}

	if rule, ok := s.rules[height]; ok {
		if rule.fatal {
			return nil, &DataError{Kind: ErrKindFatal, Height: height,
				Reason: "data source marked this height as permanently failed"}
		}
		if rule.temporaryLeft > 0 {
			rule.temporaryLeft--
			return nil, &DataError{Kind: ErrKindTemporary, Height: height,
				Reason: fmt.Sprintf("simulated transient failure, %d attempt(s) remain", rule.temporaryLeft)}
		}
	}

	b := chain[height]
	if err := ValidateBlock(b); err != nil {
		return nil, err
	}
	return cloneBlock(b), nil
}

// Tip 实现 DataSource：返回当前链尖区块的深拷贝。
func (s *SimSource) Tip(_ context.Context) (*Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBlock(s.chains[s.tipHash][len(s.chains[s.tipHash])-1]), nil
}

func cloneBlock(b *Block) *Block {
	if b == nil {
		return nil
	}
	cp := *b
	if b.Transactions != nil {
		cp.Transactions = append([]Transaction(nil), b.Transactions...)
	}
	return &cp
}
