# onchain-block-indexer-go

本地可回滚的链上区块与交易索引流水线。使用本地生成的模拟链（`SimChain`）作为数据源，
保证正常推进、重复摄入与链重组之后的索引结论一致且可解释。

## 结构

```
internal/indexer/
  types.go      区块/交易类型与可区分的错误分类（ErrorKind）
  store.go      索引存储：校验、幂等写入、回滚、检查点持久化
  source.go     数据源抽象与本地模拟链（分叉与故障注入）
  pipeline.go   摄入流水线：并发拉取、按序提交、重组回滚、指标
  pipeline_test.go  覆盖正常/重复/重组/并发/故障/恢复的测试
```

## 数据源约定

- `Source` 接口：`FetchBlock(height)` 按高度拉取区块，`Tip()` 返回链尖高度。
- 高度从 1 开始连续编号；每个区块必须携带 `Height / Hash / ParentHash`，
  交易必须携带 `Hash / From / To`。字段缺失或区块内交易哈希重复属于
  **非法数据**，不会被静默丢弃，而是以 `KindInvalidData` 失败返回。
- `SimChain` 由 seed 确定性生成：同一 seed 同一高度内容恒定；
  `Fork(h)` 后高度 h 起切换为新分支（哈希不同、父哈希衔接）；
  `InjectTransient(n)` / `InjectFatalAt(h)` 注入临时/不可恢复失败。

## 重组与回滚规则

- 提交前比对：同高度同哈希 → 幂等跳过（计 `Duplicates`）；
  同高度不同哈希或父哈希与链尖不符 → 进入重组处理。
- 重组时从冲突高度向下寻找索引与数据源一致的**共同祖先**，
  回滚深度 = 当前链尖 − 共同祖先高度。
- 深度 ≤ `MaxReorgDepth`：`RollbackTo(祖先)` 删除其上的全部区块与交易
  （被抛弃分支无任何残留），随后同步重建受影响区间并继续按序提交。
- 深度 > `MaxReorgDepth`：返回 `KindReorgTooDeep` 明确拒绝，
  索引保持原状，绝不静默继续。

## 错误分类

| 分类 | 含义 | 处理 |
|---|---|---|
| `invalid-data` | 区块/交易字段缺失或非法 | 不计入索引，立即失败并计数 `InvalidBlocks` |
| `transient-source` | 数据源临时失败 | 自动重试（`MaxRetries` 次），计数 `TransientErrs` |
| `fatal-source` | 数据源不可恢复失败 | 不重试，终止本次同步，计数 `FatalErrs` |
| `reorg-too-deep` | 重组深度超限 | 拒绝并保持索引原状 |
| `internal` | 索引器内部错误（如检查点写失败、高度不连续） | 终止并返回 |

所有错误为 `*indexer.Error`，携带高度、区块哈希与原因，可用 `KindOf(err)` 判定类别。

## 配置项（`Config`）

| 字段 | 说明 | 默认 |
|---|---|---|
| `StoreDir` | 检查点持久化目录（`checkpoint.json`，原子写入） | 必填 |
| `MaxReorgDepth` | 允许的最大重组深度，超过即拒绝 | 0（任何重组都拒绝，建议显式设置） |
| `Workers` | 并发拉取协程数 | 1 |
| `PollBlocks` | 本次同步目标高度上限，0 表示跟到数据源链尖 | 0 |
| `MaxRetries` | 临时失败最大重试次数 | 5 |

## 并发与恢复

- 多个拉取协程并发取数，提交由单一把关者按高度顺序串行执行，
  查询（`BlockAt` / `TxByHash` / `Tip`）只读已提交状态，无竞态、无中间破损。
- 每次写入/回滚后原子持久化检查点；重启后 `NewIndexer` 从已持久化的
  链尖继续，不重复、不丢失。
- 关键指标通过 `Indexer.Met`（原子计数）暴露：已应用区块/交易、重复、
  重组次数、回滚区块数、临时/不可恢复失败、非法区块。

## 验证方法

```bash
go build ./...
go test ./...          # 功能测试
go test -race -v ./... # 竞态检测 + 查看高度/区块标识/判定依据日志
```

测试覆盖：正常推进、重复摄入幂等、浅重组回滚重建、超限重组拒绝、
并发摄入（8 workers / 200 块，与串行结果逐块比对）、数据源临时失败重试、
不可恢复失败后的重启恢复、非法数据分类。
