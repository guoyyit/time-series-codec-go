# time-series-codec-go

面向长期高吞吐写入场景的时序数据压缩存储引擎。数据按块压缩落盘，
支持无损/有损两种编码、乱序与重复写入、任意时间区间随机查询、
崩溃恢复与后台块合并。写入路径与查询路径解耦。

## 快速开始

```go
cfg := codec.DefaultConfig("/var/lib/mytsdb")
cfg.Mode = codec.ModeLossy   // 或 ModeLossless
cfg.Epsilon = 0.1            // 有损误差上界
cfg.BlockMaxSamples = 1024   // 每块最大样本数
s, _ := codec.Open(cfg)
defer s.Close()

s.Write(1700000000000, 23.5) // 支持乱序、重复、迟到样本
samples, _ := s.Query(1700000000000, 1700000009999)
```

## 压缩语义

### 块结构

样本在内存缓冲（memtable）中排序去重后，按 `BlockMaxSamples` 封块。
每个块**自描述、可独立解码**——头部携带编码模式与量化步长，
因此任意区间查询无需从头重放，且配置变更不影响旧块解码。

块格式（version=1/2，头部布局相同，区别仅在有损值流）：

```
[0:2]  magic "TB"          [2]  version=1
[3]    mode (0=无损 1=有损) [4:12] epsilon (float64)
[12:16] count              [16:24] minT   [24:32] maxT
[32:]  时间戳流 || 值流     末尾 4 字节 CRC32
```

- **时间戳**：delta-of-delta + zigzag varint，始终无损。
- **值（无损模式，v1）**：Gorilla 风格 XOR 位流，解码与原始点
  **逐比特一致**，NaN 与 ±Inf 同样原样保留。
- **值（有损模式，新写入与合并产出使用 v2）**：逐样本标签流：
  - 标签 0（量化）：`q = round(v / Epsilon)` 的增量 zigzag varint，
    解码值 `q * Epsilon`；
  - 标签 1（精确）：原始 float64 的 8 字节比特，逐比特保留。
  编码时先尝试量化，**仅当重建值满足 `|decoded - v| <= Epsilon`**
  （且 `q` 在 int64 可表示范围内、重建不溢出）才用量化；否则回退
  标签 1。因此对**任意有限 float64**（含 1e18 量级、最小正 Epsilon
  等极端组合）误差上界严格成立，无法满足时静默保留原值，绝不超差。

### 特殊值规则（有损/无损统一，作用于写入、查询、合并、恢复各路径）

- **NaN**：逐比特保留（含 NaN 的载荷比特），解码结果仍为同一 NaN；
- **+Inf / -Inf**：逐比特保留，解码仍为同号无穷；
- 特殊值不参与量化，不产生"NaN 变成 0""Inf 变成有限数"之类的偏差；
- 后台合并对旧块重新编码时采用**稳定判定**：只有重建值与输入
  逐比特相同才允许量化，否则精确保留。因此特殊值不丢失、有限样本
  的误差不因反复合并而累积退化，合并前后相同区间查询结果逐点一致。

**旧格式兼容**：version=1 的有损块（早期 `q` 增量序列，无标签）
仍按原规则解码，既有落盘数据行为不变；version=2 仅用于新写入与
合并产出的新块，旧无损块始终是 version=1。

块间不建立解码依赖（相邻块可独立解码），这是随机区间查询
（需求：不得从头重放）与后台合并（单块替换）的前提。

### 写入语义：乱序、重复、迟到

- 重复时间戳采用 **last write wins**：后写覆盖先写。
- 乱序/迟到样本进入 memtable 排序；封块后到达的迟到样本会形成
  时间范围重叠的新块，查询时按裁决规则合并，不会出现块内顺序
  错误或查询漂移。
- 裁决规则（查询与合并一致）：**块 ID 大者（封块更晚）覆盖小者，
  memtable 覆盖一切块**。该规则与"全量重放 + 后写覆盖"严格等价。

### 查询语义

`Query(start, end)` 返回闭区间内升序、去重的样本：
找出所有与区间重叠的块（索引过滤），按 ID 升序应用后用 memtable
覆盖。块边界与乱序样本附近的结果与全量重放一致（有测试保证）。

## 持久化与崩溃恢复

- 每次写入先追加 **WAL**（定长记录 + CRC，可选 fsync），再入 memtable。
- 封块顺序：**块文件落盘（tmp+fsync+rename）→ 索引原子提交
  （tmp+fsync+rename）→ WAL 截断**。
- 崩溃情形：
  - 封块中途崩溃 → 块文件未被索引引用，启动时作为**孤儿清理**，
    WAL 重放恢复数据；
  - 索引提交后、WAL 截断前崩溃 → 重放产生重复样本，由去重语义
    吸收，结果不变；
  - WAL 尾部半条记录 → 重放遇 CRC 失败即截断，不污染数据；
  - 索引引用缺失块文件 → Open 报错（按上述顺序正常不会发生）。

## 后台合并

`MergeNow()`（或配置 `Merge.IntervalMillis` 自动触发）将重叠/相邻的
小块重压缩为大块回收空间：

1. 从索引快照选取**重叠闭包**分量（保证组外无与组范围重叠的块，
   否则新块的高 ID 会错误覆盖组外块中更新的写入）；
2. 无锁解码、按裁决规则合并、按当前配置重新编码；
3. 提交时校验合并范围内无并发封入的新块，**索引切换与旧文件删除
   在同一写锁内完成**——并发查询要么看到旧索引+旧文件，要么看到
   新索引+新文件，绝不暴露中间状态；校验失败则安全放弃，下轮重试。

## 配置说明

| 配置 | 含义 | 变更影响 |
|---|---|---|
| `Dir` | 数据目录 | — |
| `Mode` | 无损/有损 | 仅影响新块；旧块按各自头部模式解码 |
| `Epsilon` | 有损误差上界，必须为正的有限数 | 仅影响新块；旧块按各自头部 epsilon 解码 |
| `BlockMaxSamples` | 每块最大样本数 | 仅影响新块与合并输出 |
| `Merge.IntervalMillis` | 自动合并间隔，≤0 禁用 | 即时生效 |
| `Merge.MinBlocks` | 触发合并的最小块数 | 即时生效 |
| `SyncWrites` | 每次写入 fsync WAL | 即时生效；关闭可提速但降低崩溃持久性 |

**配置变更是安全的**：块头自描述编码参数，任何配置组合下重启后
既有数据均可正确解码（有 `TestConfigChangeKeepsOldDataDecodable` 保证）。

### Epsilon 合法范围与校验

有损模式的 `Epsilon` 必须是**正的有限数**。以下取值都会在 `Open`
时被拒绝并返回可区分错误（可用 `errors.Is(err, codec.ErrInvalidEpsilon)`
或 `errors.Is(err, codec.ErrInvalidConfig)` 判定）：

- `0` 或负数；
- `NaN`；
- 正无穷 `+Inf`、负无穷 `-Inf`。

校验在任何文件系统操作之前执行，**校验失败不会创建或修改数据目录
下的任何文件或目录**（有 `TestInvalidEpsilonRejected` 保证）。
`math.SmallestNonzeroFloat64` 等极小正值是合法的：此时普通有限样本
基本都会因无法量化而逐比特保留，承诺仍然成立（不超差、不改写）。

## 容量估算

原始点 16 字节（8 时间戳 + 8 值）。压缩后每块约：

```
块大小 ≈ 32(头) + 4(CRC) + 时间戳流 + 值流
```

- 时间戳流：固定间隔采样 ≈ 0.1–1 字节/点（dod 为 0 或极小）。
- 值流（无损 XOR）：平滑信号 ≈ 1–2 字节/点；噪声大时退化至 ~9 字节/点。
- 值流（有损 v2）：量化点为 1 字节标签 + 增量 varint，取决于值域
  变化幅度/epsilon，通常 2–3 字节/点；无法满足误差上界的样本
  （NaN/±Inf、极端量级、epsilon 极小时）占 1 + 8 = 9 字节/点，
  与无损 XOR 的最坏情况相当。含大量此类样本时，有损容量收益会
  明显下降甚至与无损持平——这是"绝不静默超差"保证的代价。

经验估算：**规则采样的平滑指标，无损约 2–4 字节/点（4–8x 压缩），
有损可再降 30–50%**。精确测算请用真实数据跑 `TestCompressionRatio`
式的基准。另需预留：WAL（未封块部分 24 字节/点，封块后截断）、
索引（每块约 100 字节 JSON）。

## 测试覆盖

`codec/` 下的自动化测试覆盖：

- 无损逐比特一致 / 有损误差上界（`block_test.go`）；
- 乱序、重复、迟到写入与去重（`TestLosslessOutOfOrderDedup`）；
- 任意区间查询 vs 全量重放参考模型、块边界（`TestBlockBoundaryQueries`）；
- 崩溃恢复：WAL 重放、孤儿块清理、WAL 撕裂尾部（`TestCrashRecovery*`）；
- 后台合并：块数收敛、并发读写下与参考模型最终一致
  （`TestMergeReducesBlocks`、`TestMergeConcurrentReadWrite`，`-race` 通过）；
- 配置变更后旧数据可解码（`TestConfigChangeKeepsOldDataDecodable`）。
- 有损数值保证（`lossy_guarantee_test.go`、`lossy_store_test.go`）：
  NaN（含载荷）/±Inf 逐比特保留；1e18 量级与 0.1/最小正 epsilon
  等极端组合不超差；特殊与极端样本经封块、乱序去重、区间查询、
  重启恢复、后台合并、配置切换后与全量重放参考模型逐点一致；
  非法 epsilon 被拒绝（`ErrInvalidEpsilon`）且数据目录零残留；
  随机乱序写入 + 随机区间交叉验证、连续多轮崩溃重启、合并并发
  读写一致性（`TestLossyRandomSpecialCrossValidation`、
  `TestLossyRepeatedCrashRestart`、`TestLossyMergeConcurrentSpecial`）。

```sh
go test -race ./...
```
