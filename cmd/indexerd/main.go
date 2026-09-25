package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/h3yow/onchain-block-indexer-go/internal/chain"
	"github.com/h3yow/onchain-block-indexer-go/internal/indexer"
	"github.com/h3yow/onchain-block-indexer-go/internal/store"
)

func main() {
	snapshot := flag.String("snapshot", "index-snapshot.json", "索引快照文件路径（重启后从该进度继续）")
	maxReorg := flag.Int64("max-reorg-depth", 5, "允许回滚的最大重组深度")
	flag.Parse()

	logger := log.New(os.Stdout, "[indexer] ", log.LstdFlags|log.Lmicroseconds)
	logf := func(format string, args ...any) { logger.Printf(format, args...) }

	src := chain.NewSimSource()
	st, err := store.OpenFileStore(*snapshot)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	ix := indexer.New(indexer.Config{
		MaxReorgDepth: *maxReorg,
		Logf:          logf,
	}, src, st)
	ctx := context.Background()

	// 模拟数据源：推进到高度 5。
	src.ExtendN(5)

	if err := ix.CatchUp(ctx); err != nil {
		log.Fatalf("catch up: %v", err)
	}

	// 在高度 3 制造浅分叉（回滚 2 个区块，重建 3 个新区块）。
	newTip := src.Reorg(3, 3)
	logf("simulated reorg at fork height=3, new tip=%s", newTip)
	if err := ix.CatchUp(ctx); err != nil {
		log.Fatalf("catch up after reorg: %v", err)
	}

	m := ix.Metrics()
	fmt.Printf("metrics: blocks=%d txs=%d reorgs=%d rollbacks=%d tempErrs=%d fatalErrs=%d dup=%d\n",
		m.BlocksIndexed, m.TxsIndexed, m.Reorgs, m.Rollbacks,
		m.TemporaryErrors, m.FatalErrors, m.DuplicateIngest)
}
