# onchain-block-indexer-go

本地可回滚的链上区块与交易索引流水线。使用本地生成的模拟链（`SimChain`）作为数据源，
保证正常推进、重复摄入与链重组之后的索引结论一致且可解释。

## 结构

```
internal/indexer/
  types.go         区块/交易类型与可区分的错误分类（ErrorKind）
  store.go         索引存储：校验、幂等写入、回滚、一致快照、多余数据清理、检查点持久化
  source.go        数据源抽象与本地模拟链（分叉与故障注入）
  pipeline.go      摄入流水线：并发拉取、按序提交、重组回滚、指标、留痕接入
  audit.go         重组留痕（旁路审计）：持久化、反查、容量裁剪
  inspect.go       巡检对账：只读快照逐高度核对，按类别出具可解释结论
  repair.go        就地自愈：回滚到首个不一致位置并按序重建
  pipeline_test.go 覆盖正常/重复/重组/并发/故障/恢复的测试
  inspect_test.go  覆盖巡检分类/自愈等价/幂等/超限拒绝/留痕/并发竞态的测试
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

## 巡检对账（只读）

随时可手动调用 `Indexer.Inspect(ctx)`，拿当前索引与本地数据源做逐高度核对。

- **判定口径（问题逐类分开，不混成笼统失败）**：
  | 类别 | 判定依据 |
  |---|---|
  | `missing-height` | 索引链尖以内本该存在的高度缺失（真正的缺口） |
  | `content-mismatch` | 同高度区块哈希对不上，或哈希相同但交易集合对不上 |
  | `parent-broken` | 索引内部相邻块父链衔接断裂（`ParentHash` 与前一高度区块哈希不符） |
  | `extra-height` | 高度只存在于索引、高出数据源链尖 |
  | `extra-tx` | 交易只存在于索引、不属于任何已提交区块（悬挂交易） |
- 巡检先在**单个读锁内取一份已提交快照**（`Store.Snapshot`），再与数据源比对：
  只读到完整已提交状态，绝不会读到摄入写一半的中间状态；快照之后摄入继续推进
  也不影响本次结论。
- **索引链尖落后数据源链尖属于正常追赶（`Report.Lag`），不计为问题**，
  因此巡检不会把正在正常写入的内容误判成脏数据。
- 索引干净时巡检**完全只读**（不改检查点、不留痕、不动任何持久化状态）；
  发现不一致时，`Report` 给出每类问题的高度、索引/数据源区块标识、
  首个不一致高度（`FirstMismatch`）、影响区间与一句话结论（`Summary()`）。

## 就地自愈

发现不一致时调用 `Indexer.Repair(ctx)`，流程为：

1. 与正常摄入共用同一把提交锁（互斥），先巡检出具结论；索引健康则**零改动**返回。
2. 清理高出当前链尖的多余高度（`PurgeAbove`）。
3. 存在链内不一致时，**回滚到第一个不一致位置之前**（`FirstMismatch-1`），
   被放弃区块与摄入重组走同一套留痕路径；然后从回滚点**按顺序重建**到数据源链尖。
4. 清理仍高于数据源链尖的多余高度，并清理不属于任何区块的悬挂交易。

- **等价性**：修复后按高度、按交易查到的结论，与「清空目录从头全量重放」
  逐高度哈希、逐交易归属、总块数/总交易数完全一致；重建走的是与正常摄入
  相同的幂等写入路径，因此**反复修复不产生重复记录、计数不漂移**，
  第二次起巡检判健康直接零改动（天然幂等）。
- **触发条件与边界**：只有巡检发现 `missing/mismatch/parent-broken/extra-*`
  才动作；单纯的链尖滞后不触发修复。回滚深度 = 修复前链尖 − 回滚点，
  与摄入重组同一口径，**受 `MaxReorgDepth` 约束；超限在任何改动之前返回
  `KindReorgTooDeep`，索引保持原样、也不写留痕**。
- 重建期间数据源临时失败沿用既有重试策略；不可恢复失败会使重建停在
  一个一致的较短前缀（已提交前缀始终可用，重新修复即可续上）。

## 重组留痕（旁路审计）

每次重组（摄入触发或自愈触发，共用 `abandonBranchLocked`）都会把被放弃的
旧分支区块写入独立文件 `audit-log.json`（与 `checkpoint.json` 分离，原子写），
记录：单调编号、重组序号、被放弃高度、区块/父哈希、全部交易哈希、
**稳定的被放弃分支标识 `BranchID`**（由该分支最低区块派生）、共同祖先高度
与新链尖高度。

- **重启可查**：`ByHeight(h)` / `ByBlock(hash)` / `ByTx(txHash)` / `All()`，
  可按高度或交易反查它曾经属于哪个已被放弃的分支。
- **纯旁路**：留痕不参与 `BlockAt` / `TxByHash` / `Tip` 的判定，
  被放弃区块不会借留痕重新出现在常规查询结果里；留痕写失败只累计
  `AuditFailures` 指标，不阻断索引主流程（索引正确性优先）。
- **容量控制（`Config.Audit`，`AuditConfig`）**：
  | 字段 | 规则 |
  |---|---|
  | `Enabled` | false（默认）不记录，`Indexer.Audit()` 返回 nil，其方法均为空操作 |
  | `MaxEntries` | 条数上限，<=0 不限；超限在写入同一次原子持久化内裁剪最旧记录 |
  | `RetainBlocks` | 按高度保留窗口（相对已知最新链尖往回），<=0 不限；窗口外旧记录允许清理 |
- 也可手动 `AuditLog.Prune()` 立即按规则裁剪。清理只动留痕文件，
  **绝不触碰索引检查点，不影响现有索引正确性**；写入与清理在同一把互斥锁内、
  单次原子 rename 落盘，并发下不会把留痕写坏。
- 与重组深度上限的兼容：留痕只在重组通过深度校验、真正执行回滚后产生；
  超限拒绝不留痕。

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
| `Audit` | 重组留痕配置 `AuditConfig{Enabled, MaxEntries, RetainBlocks}` | 关闭 |

## 并发与恢复

- 多个拉取协程并发取数，提交与自愈由单一把关者按高度顺序串行执行
  （摄入重组与修复共用同一把锁），查询（`BlockAt` / `TxByHash` / `Tip`）只读
  已提交状态，巡检读的是单读锁内拷出的一致快照——无竞态、无中间破损。
- 每次写入/回滚/清理后原子持久化检查点；重启后 `NewIndexer` 从已持久化的
  链尖继续，不重复、不丢失；留痕文件同样在重启后恢复可查。
- 关键指标通过 `Indexer.Met`（原子计数）暴露：已应用区块/交易、重复、
  重组次数、回滚区块数、临时/不可恢复失败、非法区块，以及旁路留痕的
  写入条数 `AuditRecords` 与失败次数 `AuditFailures`。

## 本地复现与验证

```bash
go build ./...
go test ./...              # 功能测试（含全部新增用例）
go test -race -v ./...     # 竞态检测 + 查看各用例高度/区块标识/判定依据日志
go test -race -count=10 \
  -run 'TestInspectRepairConcurrentWithIngestion|TestConcurrentIngestion|TestAuditRetention' ./...
                           # 并发用例多轮加压
```

原有覆盖（不回归）：正常推进、重复摄入幂等、浅重组回滚重建、超限重组拒绝、
并发摄入（8 workers / 200 块，与串行结果逐块比对）、数据源临时失败重试、
不可恢复失败后的重启恢复、非法数据分类。

新增覆盖：

- 健康巡检只读且不改检查点；链尖滞后不误判；五类问题逐类列出并给出定位
  （`TestInspectHealthyReadOnly` / `TestInspectIssueCategories`）。
- 分叉后自愈的按高度/按交易结论与全新目录全量重放逐块逐交易一致；
  重复修复幂等、计数不漂移（`TestRepairEqualsFullReplay`）。
- 超限重组时修复在任何改动之前被拒绝、索引原样、不留痕
  （`TestRepairRejectedWhenTooDeep`）；混合污染（缺口/断链/多余高度/多余交易）
  修复后同样等价全量重放（`TestRepairTamperedCategories`）。
- 留痕在常规重组后可按高度/交易/区块反查分支，重启后仍可查，且被放弃区块
  不复活到常规查询；条数上限、高度窗口裁剪与并发写入不损坏
  （`TestAuditTrailLifecycle` / `TestAuditRetention`）。
- 巡检/修复与摄入并发（含分叉）：读不到缺口/断链/多余的中间态，最终与分叉链
  全量重放一致（`TestInspectRepairConcurrentWithIngestion`）。

## 典型用法

```go
ix, _ := indexer.NewIndexer(indexer.Config{
    StoreDir: "./data", MaxReorgDepth: 32, Workers: 8,
    Audit: indexer.AuditConfig{Enabled: true, MaxEntries: 10000},
}, src)
_ = ix.Run(ctx)

// 随时手动巡检（只读）
rep, _ := ix.Inspect(ctx)
fmt.Println(rep.Summary())
for _, iss := range rep.Issues { // 按类别区分、带高度与区块标识
    fmt.Println(iss)
}

// 发现不一致就地自愈（受 MaxReorgDepth 约束）
if _, res, err := ix.Repair(ctx); err == nil {
    fmt.Printf("回滚=%d 重建=%d 清理高度=%d 清理交易=%d\n",
        res.Reverted, res.Rebuilt, res.PurgedHeights, res.PurgedTxs)
}

// 事后追查：某笔交易曾属于哪个被放弃的分支
for _, e := range ix.Audit().ByTx("0x...") {
    fmt.Printf("height=%d block=%s branch=%s\n", e.Height, e.BlockHash, e.BranchID)
}
```
