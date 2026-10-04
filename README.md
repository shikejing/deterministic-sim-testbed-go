# deterministic-sim-testbed-go

单进程确定性仿真测试台：把并发与故障场景从真实环境搬进仿真，
用受控的时间推进与调度替代真实时钟和真实网络。
**给定种子后，重复运行、进程重启、换机器运行，事件序列与终态逐条一致。**

## 核心思想

- **虚拟时钟**：时间只随事件推进（`Time` 为滴答数），没有真实时钟。
- **单线程调度循环**：所有事件在一个按 `(Time, ID)` 排序的最小堆中排队，
  同刻事件严格按创建顺序执行，调度顺序完全确定。
- **单一随机源**：splitmix64（完整 2^64 周期，长期推进不发散）。
  定时器触发、消息送达、故障判定、节点内随机决策全部从这一个随机源按固定顺序取数。
- **状态哈希**：每一步执行后计算全状态哈希（时钟、随机源、节点状态、事件队列、网络），
  写入事件日志，用于逐条比对两次运行是否一致。

## 复现语义

一次运行的结果由且仅由以下内容决定：

1. `Config.Seed` —— 随机源种子；
2. `Config` 的全部配置项（上限、故障概率等）；
3. 注册进仿真的节点与初始注入（`RegisterNode` / `InjectClient` 的调用序列）。

满足以上三点，任意时刻重跑都会得到相同的：事件执行顺序、消息送达顺序、
随机选择序列、对外效应序列与终态哈希（`Result.FinalHash`）。
复现不依赖真实时间与真实网络。

被测节点（`Node` 接口）必须遵守契约，否则确定性不成立：

- 只能通过 `Context` 感知时间（`Now`）、获取随机性（`Rand`）、通信（`Send`/`After`）；
- 不得读取真实时钟、真实网络、包级随机源或启动额外 goroutine；
- 状态必须能通过 `Snapshot`/`Restore` 完整往返；
- 出现在 `Event.Payload` / `Message.Body` 中的自定义类型需用 `sim.RegisterTypes` 注册。

## 故障注入

所有故障的发生时机与概率由 `FaultConfig` 与种子共同决定；
每个被注入的事件带 `Fault` 标注，日志条目的 `Note` 字段给出可定位的注入点说明
（含事件 ID，如 `fault:drop msg n1->n2 kind=replicate (event #137)`）。

| 故障 | 配置项 | 注入点 |
|---|---|---|
| 消息丢失 | `DropRate` | 发送时判定，事件标注 `drop`，投递时记录 |
| 消息重复 | `DupRate` | 发送时额外复制，事件标注 `dup` |
| 消息乱序 | `ReorderRate` + `MaxReorderDelay` | 发送时追加随机延迟 |
| 网络分区/恢复 | `PartitionRate` + `PartitionLen` | 每步判定，切断/恢复双向链路 |
| 节点崩溃/重启 | `CrashRate` + `RestartDelay` | 每步判定，崩溃期间消息与定时器被丢弃，重启后重新 `Init` |
| 时钟跳变 | `ClockJumpRate` + `MaxClockJump` | 每步判定，累计偏移作用于 `Context.Now()` |

## 不变式校验

`AddInvariant(name, fn)` 注册的不变式按 `InvariantEvery` 周期校验。
违反时运行立即停止（`ReasonInvariant`），`Result.Violation` 携带最小复现信息：
不变式名称、**种子**、发生步数、具体描述、**最近 10 条关键事件序列**。
用该种子与同一 setup 重跑即可复现到同一步。

`sim/kv.go` 提供三个示例不变式（也是测试的被测系统）：

- `ConfirmedDurable`：已确认的结果不能丢失（须存在于至少 quorum 个节点）；
- `NoDuplicateApply`：同一操作不能重复生效（每节点每操作至多生效一次）；
- `ValuesConsistent`：任意时刻可见状态自洽（同 key 不得有冲突值）。

## 快照、恢复与重放

- `Snapshot()` 序列化完整逻辑状态（随机源、队列、节点、网络、时钟偏移）；
  `CheckpointEvery`/`MaxCheckpoints` 控制运行期自动检查点。
- `Restore(data, factories)` / `ResumeFrom(...)` 从快照恢复；
  恢复后继续推进与从未中断的运行**逐条一致**（测试覆盖）。
  快照带格式版本号，版本不兼容时返回 `ErrSnapshotVersion`，
  绝不会悄悄读出不完整数据。恢复后可用 `Configure` 调整运行上限再继续。
- `Replay(cfg, setup, log, upto)` 从种子与事件日志重放：
  逐步校验哈希、逐条校验对外效应；`upto` 可截断到任意已记录步。
  重放全程不接触真实时钟与网络；`Context.Emit` 的对外效应只写入内存日志，
  重放时进入独立缓冲区用于比对，**不会再次触发真实副作用**。

## 终止语义与资源上限

`Run` 在以下任一情况停止，且总是停在自洽、可快照的状态：

| 停止原因 | 触发条件 |
|---|---|
| `queue-empty` | 事件耗尽，自然结束 |
| `max-steps` | 达到 `MaxSteps` |
| `max-events` | 达到 `MaxEvents`（已创建事件总数） |
| `max-time` | 下一事件超出 `MaxTime` |
| `canceled` | 外部 `context` 取消（在事件边界检查，不会留下写了一半的状态） |
| `invariant-violated` | 不变式违反 |

资源控制：

- `MaxInFlight`：在途事件上限（并行度旋钮），超出的事件被丢弃并计入
  `Result.DroppedBackpressure`，模拟背压并限制内存；
- `LogCap`：事件日志容量（环形淘汰最旧），`MaxCheckpoints`：检查点数量上限；
- 状态哈希对节点使用可选的 `StateHasher` 接口（顺序无关、O(条目数)），
  对队列只哈希 `(ID, Time, Kind, Fault)`，长时间运行每步开销可控。

## 配置项一览

```go
type Config struct {
    Seed            uint64      // 随机种子：复现的总开关
    MaxSteps        uint64      // 最大执行步数（0=不限）
    MaxEvents       uint64      // 最大事件创建数（0=不限）
    MaxTime         Time        // 虚拟时间上限（0=不限）
    MaxInFlight     int         // 在途事件上限/并行度（0=不限）
    Faults          FaultConfig // 故障概率与参数（见上表）
    InvariantEvery  uint64      // 不变式校验间隔（0 或 1 = 每步）
    CheckpointEvery uint64      // 检查点间隔（0=不保存）
    MaxCheckpoints  int         // 检查点保留上限（默认 8）
    LogCap          int         // 日志容量（0=不记录，<0=不限）
}
```

## 运行测试与演示

```sh
go test ./...                                    # 确定性/故障注入/快照恢复/重放/终止/背压
go run ./cmd/simdemo -seed 42 -replay -snapshot-check
```

`cmd/simdemo` 在注入全部六类故障的情况下运行一个多数派确认的复制 KV，
打印终态哈希；同一组参数在任何机器上输出完全相同。

## 目录结构

```
sim/            测试台核心包
  sim.go        调度循环、状态哈希、检查点
  types.go      事件、结果等基础类型
  rng.go        splitmix64 确定性随机源
  queue.go      (Time, ID) 有序事件堆
  node.go       Node 契约与 Context
  network.go    分区模型
  faults.go     故障注入判定
  log.go        有容量上限的事件/效应日志
  snapshot.go   快照与版本化恢复
  replay.go     重放与检查点续跑
  invariant.go  不变式与违反报告
  kv.go         演示工作负载：多数派确认的复制 KV + 示例不变式
  config.go     全部配置项
cmd/simdemo/    演示程序
```
