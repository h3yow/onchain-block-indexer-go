package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// Source 是区块数据源抽象。
type Source interface {
	// FetchBlock 按高度拉取区块；临时失败返回 KindTransientSource，
	// 不可恢复失败返回 KindFatalSource。
	FetchBlock(height uint64) (Block, error)
	// Tip 返回数据源当前链尖高度。
	Tip() uint64
}

// SimChain 是本地生成的模拟链数据源，支持分叉与故障注入。
// 链内容完全由 seed 确定：同一 seed 的同一高度区块内容恒定，
// Fork 之后 forkHeight 起的区块切换到新分支（内容不同、父哈希衔接）。
type SimChain struct {
	mu         sync.Mutex
	seed       string
	length     uint64
	forkHeight uint64 // 0 表示无分叉
	forked     bool
	transientN int // 接下来 N 次 FetchBlock 返回临时失败
	fatalAt    map[uint64]bool
}

// NewSimChain 生成一条长度为 length 的确定性模拟链。
func NewSimChain(seed string, length uint64) *SimChain {
	return &SimChain{seed: seed, length: length, fatalAt: make(map[uint64]bool)}
}

// Tip 实现 Source。
func (c *SimChain) Tip() uint64 { return c.length }

// Fork 在 height 处制造分叉：该高度及之后的区块将被替换为新分支。
func (c *SimChain) Fork(height uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forkHeight = height
	c.forked = true
}

// InjectTransient 让接下来 n 次拉取返回临时失败。
func (c *SimChain) InjectTransient(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transientN += n
}

// InjectFatalAt 让指定高度的拉取永远返回不可恢复失败。
func (c *SimChain) InjectFatalAt(height uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fatalAt[height] = true
}

// branch 返回高度 height 所属分支标识。
func (c *SimChain) branch(height uint64) string {
	if c.forked && height >= c.forkHeight {
		return "fork"
	}
	return "main"
}

func hashOf(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// buildBlock 确定性构造指定高度的区块（须在锁内或只读场景调用）。
func (c *SimChain) buildBlock(height uint64) Block {
	br := c.branch(height)
	parent := "genesis"
	if height > 1 {
		parent = hashOf(c.seed, c.branch(height-1), fmt.Sprint(height-1))
	}
	b := Block{
		Height:     height,
		Hash:       hashOf(c.seed, br, fmt.Sprint(height)),
		ParentHash: parent,
	}
	for i := 0; i < 2; i++ {
		b.Txs = append(b.Txs, Tx{
			Hash:   hashOf(b.Hash, "tx", fmt.Sprint(i)),
			From:   hashOf(c.seed, "from", fmt.Sprint(height), fmt.Sprint(i)),
			To:     hashOf(c.seed, "to", fmt.Sprint(height), fmt.Sprint(i)),
			Amount: 100*height + uint64(i),
		})
	}
	return b
}

// FetchBlock 实现 Source。
func (c *SimChain) FetchBlock(height uint64) (Block, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fatalAt[height] {
		return Block{}, &Error{
			Kind: KindFatalSource, Height: height,
			Reason: "数据源对该高度返回不可恢复失败（注入）",
		}
	}
	if c.transientN > 0 {
		c.transientN--
		return Block{}, &Error{
			Kind: KindTransientSource, Height: height,
			Reason: "数据源临时不可用（注入）",
		}
	}
	if height == 0 || height > c.length {
		return Block{}, &Error{
			Kind: KindFatalSource, Height: height,
			Reason: fmt.Sprintf("高度超出数据源范围 [1,%d]", c.length),
		}
	}
	return c.buildBlock(height), nil
}
