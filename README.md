# 本地可回滚的链上区块与交易索引流水线

一个用**本地生成的链数据**模拟链数据源的区块/交易索引器（Go，仅标准库）。
支持按高度顺序摄入、幂等重复摄入、链重组回滚重建、并发安全摄入、
数据源故障分类、基于快照的崩溃恢复，并通过指标与结构化日志让每一次
索引结论都可解释。

## 目录结构

```
internal/chain/        链领域模型、数据源接口与本地模拟数据源（含分叉/故障注入）
  types.go             Block / Transaction 与字段校验（缺失/非法 -> 可区分错误）
  source.go            DataSource 接口、DataError 三分类
  sim.go               SimSource：确定性区块/交易生成、Reorg、FailTemporary/FailFatal
internal/store/        索引持久化
  store.go             Store 接口（Head/BlockAt/BlockByHash/Tx/ApplyBlock/TruncateFrom）
  mem.go               内存实现：原子 Apply、回滚无残留
  file.go              JSON 快照实现：临时文件 + fsync + 原子 rename
internal/indexer/      摄入与重组流水线
  indexer.go           Ingest / CatchUp / 共同祖先探测 / 深度上限 / 查询接口
  metrics.go           关键运行指标（原子计数）
cmd/indexerd/          演示入口
```

## 数据源约定（DataSource contract）

`chain.DataSource` 抽象数据来源，模拟实现 `SimSource` 遵守同样约定：

1. `Tip(ctx)` 返回当前规范链的链尖区块。
2. `BlockAt(ctx, height, tipHash)` 按**链尖视角**返回区块：
   `tipHash` 决定从哪条分叉读历史；空字符串表示当前规范链。
   这模拟了真实链客户端"重组发生时，在新链尖视角下读取新链历史"的语义。
3. 区块按高度连接：高度 0 为创世块（约定哈希 `genesis`，`ParentHash` 为空）；
   高度 > 0 的区块必须有非空 `ParentHash` 指向前一高度区块。
4. 区块哈希格式 `0x` + 64 位十六进制；地址格式 `0x` + 40 位十六进制；
   交易 `Value` 不得为负；同一区块内交易哈希不得重复。空区块（无交易）合法。
5. 所有失败都包装成 `*chain.DataError` 返回，**绝不静默丢弃任何区块或交易**。

模拟数据源的测试辅助方法：

| 方法 | 用途 |
| --- | --- |
| `Extend()` / `ExtendN(n)` / `ExtendEmpty()` | 在规范链尾部追加区块（交易确定性唯一） |
| `Reorg(forkHeight, newLen)` | 在 `forkHeight` 处分叉：保留共同前缀，生成全新哈希/交易的新链 |
| `OverrideBlock(height, b)` | 替换非链尖区块，用于注入字段缺失/格式非法数据 |
| `FailTemporary(height, n)` | 对该高度的接下来 n 次读取注入临时失败，之后自动恢复 |
| `FailFatal(height)` / `ClearFailure(height)` | 注入/清除不可恢复失败 |

## 摄入规则与幂等性

- `Ingest(ctx)` 向链尖推进一步；`CatchUp(ctx)` 循环摄入直到追上链尖。
- 本地链头哈希 == 数据源链尖哈希：**幂等跳过**，不写任何记录，
  计入指标 `DuplicateIngest`。重复摄入同一高度永远不会产生重复区块或交易。
- 下一区块的 `ParentHash == 本地链头哈希`：原子写入区块与其全部交易。
- 提交在索引器内部由单把提交锁串行化（`commitMu`），因此**并发摄入**
  只会让一个调用者推进、其余看到幂等结果；存储层的每次 Apply/Truncate
  自身也是持锁原子操作，不存在中间破损状态。

## 重组与回滚规则

发现下一区块的 `ParentHash` 不等于本地链头时，判定发生分叉：

1. 从本地链头沿高度**向下逐块探测**：比较同一高度上"本地哈希"与
   "新链尖视角下的远端哈希"，找到第一个哈希相等的高度作为**共同祖先**
   （创世块不一致时祖先高度记为 -1）。
2. **先校验深度再改动任何数据**：回滚深度 = 本地链头高度 − 共同祖先高度。
   `depth > MaxReorgDepth` 时返回 `*indexer.ReorgTooDeepError`（其中包含
   depth、上限、旧/新链尖哈希、祖先高度），索引保持拒绝前原样，
   指标 `RejectedReorgs` +1。不会静默继续。
3. 深度允许时，原子执行 `TruncateFrom(ancestor+1)`：删除该高度及以上的
   **区块哈希索引与全部交易索引**，已抛弃数据无残留；指标 `Rollbacks`
   增加回滚块数、`Reorgs` +1。
4. 从共同祖先 +1 开始从新链重新拉取并逐块原子重建（第一次重建随本次
   重组完成，剩余区间由后续 Ingest 线性补齐）。

## 错误分类

| 类别 (`DataError.Kind`) | 含义 | 处理方式 | 指标 |
| --- | --- | --- | --- |
| `validation` | 区块/交易字段缺失或格式非法（不可恢复） | 立即失败、显式报告，不推进进度 | `FatalErrors` |
| `temporary` | 数据源临时失败（可重试，如模拟的瞬时读取失败） | `CatchUp` 按退避重试 `MaxRetries` 次，重试耗尽才返回 | `TemporaryErrors` |
| `fatal` | 不可恢复失败（数据永久缺失、未知链尖视角等） | 立即失败、不推进进度 | `FatalErrors` |

错误文本统一形如
`chain data error kind=<类别> height=<高度>: <可区分原因>`，
例如：
- `kind=validation height=1: tx 0xbbb... has invalid from address "": want 0x + 40 hex digits`
- `kind=temporary height=2: simulated transient failure, 1 attempt(s) remain`
- `kind=fatal height=6: block at height 6 permanently unavailable under tip 0x.. (chain height 5)`

## 持久化与故障恢复

- `FileStore` 在每次成功提交后把全量状态（区块记录 + 交易记录）写入
  同目录临时文件，`fsync` 后原子 `rename` 覆盖快照；崩溃最多丢失最后一次
  未完成提交，快照始终是完整状态。
- 进度就是快照中的最高高度区块。重启后 `OpenFileStore` 载入快照，
  从链头 +1 继续：**不重复**（同高度同哈希 Apply 幂等）、**不丢失**
  （已落盘区块全部可查）。
- 回滚与重建同样每次提交即落盘，重启后看到的是一致的链上区间。

## 查询与指标

稳定查询（未命中返回 `store.ErrNotFound`）：

- `Indexer.BlockAt(height)` / `BlockByHash(hash)`
- `Indexer.Tx(txHash)`：返回交易及所属区块哈希、高度、块内序号。
- `Indexer.Metrics()`：

| 指标 | 含义 |
| --- | --- |
| `BlocksIndexed` / `TxsIndexed` | 成功新写入的区块/交易数（重复摄入不计） |
| `Rollbacks` / `Reorgs` | 回滚抛弃的区块数 / 完成的重组次数 |
| `RejectedReorgs` | 因超过 `MaxReorgDepth` 被拒绝的重组次数 |
| `TemporaryErrors` / `FatalErrors` | 两类数据源失败计数 |
| `DuplicateIngest` | 幂等重复摄入次数 |

## 配置项（indexer.Config）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `MaxReorgDepth` | `10` | 允许回滚的最大区块数，超过则返回 `ReorgTooDeepError` |
| `MaxRetries` | `3` | `CatchUp` 遇到 temporary 错误时的重试次数 |
| `RetryBackoff` | `10ms` | 重试退避时长 |
| `Logf` | 静默 | 日志回调；每行带高度、区块标识与判定依据（basis） |

演示程序参数：`-snapshot`（快照路径，默认 `index-snapshot.json`）、
`-max-reorg-depth`（默认 5）。

## 验证方法

```bash
# 全量测试（含竞态检测，可重复运行）
go test -race -count=1 ./...

# 查看带高度/区块标识/判定依据的单测日志
go test ./internal/indexer/ -v

# 运行演示：顺序推进 -> 高度 3 处浅重组 -> 指标汇总
go run ./cmd/indexerd -snapshot /tmp/index-snapshot.json
```

测试覆盖场景：

1. `TestNormalAdvance`：正常按高度推进，区块/交易均可按标识查询；
2. `TestDuplicateIngest`：重复摄入幂等，无重复记录，指标正确；
3. `TestShallowReorg`：浅重组回滚到共同祖先并重建，抛弃区块/交易无残留；
4. `TestReorgExceedsLimit`：超限重组被明确拒绝，索引状态保持原样；
5. `TestConcurrentIngest`：8 goroutine 并发摄入，结果无重复无破损（竞态检测通过）；
6. `TestTemporaryFailureRetried` / `TestFatalFailureDistinct`：
   临时失败被归类并重试成功，不可恢复失败与临时失败区分处理；
7. `TestInvalidBlockNotSilentlyDropped`：非法字段产生可区分的 validation 错误；
8. `TestFileStoreRestartResumesFromSnapshot` / `TestRestartResumesWithoutRepeating`：
   重启后从持久化进度继续，不重复不丢失。
