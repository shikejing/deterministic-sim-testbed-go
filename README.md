# deterministic-sim-testbed-go

单进程确定性仿真测试台：把并发与故障场景从真实环境搬进仿真，
用受控的时间推进与调度替代真实时钟和真实网络。
给定种子后，事件的执行顺序、消息的送达顺序与所有随机选择完全确定——
重复运行、进程重启、换机器运行，结果逐条一致。

## 复现语义

确定性的来源只有三个，全部显式可控：

1. **种子（`Config.Seed`）**：引擎内置 splitmix64 随机源（不依赖 `math/rand`，
   避免 Go 版本差异），所有随机决策——消息延迟、丢失/重复/乱序判定、
   崩溃/分区/时钟跳变的时机与参数——都来自这一条随机流。
2. **事件调度顺序**：事件按 `(虚拟时间, 入队序号)` 从最小堆弹出，
   同一时间的多个事件顺序固定；事件处理是原子的，单 goroutine 执行。
3. **配置（`Config`）**：延迟区间、故障概率、运行上限等全部显式给出。

只要种子与配置相同，以下三者逐条一致：

- 每个事件的 ID、类型、虚拟时间与处理顺序；
- 每条消息的送达/丢失/重复/乱序决策；
- 每次故障注入的类型、目标与发生事件（见 `FaultRecord`，可定位到事件 ID）。

节点实现的约束：只能通过 `Context` 交互（`Send` / `SetTimer` / `Now`），
不得使用真实时钟、真实网络或全局随机源；`Snapshot` 输出必须确定。

## 快速开始

```go
cfg := sim.Config{
    Seed: 42, MinLatency: 1, MaxLatency: 50, MaxEvents: 100000,
}
cfg.Faults.DropProb = 0.2
e := sim.NewEngine(cfg)
e.AddNode("server", &sim.KVServer{})
e.AddNode("client", &sim.KVClient{Server: "server", Total: 20, Interval: 100})
e.AddInvariant("no-duplicate-apply", func(e *sim.Engine) error { ... })
res, err := e.Run(context.Background())
// res.Reason 解释为何停止；res.Faults 列出每一次故障注入
```

## 配置项含义

| 字段 | 含义 |
| --- | --- |
| `Seed` | 随机种子；同一种子运行结果逐条一致 |
| `MaxSteps` / `MaxEvents` | 调度步数 / 事件数上限，0 为不限 |
| `MaxTime` | 虚拟时间上限（纳秒），0 为不限 |
| `MinLatency` / `MaxLatency` | 消息基础延迟区间（虚拟纳秒） |
| `RecordLog` | 记录完整事件日志（用于重放） |
| `CheckpointEvery` | 每多少事件保存一个检查点快照 |
| `Faults.DropProb` | 消息丢失概率 |
| `Faults.DuplicateProb` | 消息重复概率（多投一份） |
| `Faults.ReorderProb` / `ReorderMaxDelay` | 消息乱序概率 / 最大附加延迟 |
| `Faults.CrashProb` / `CrashMinDown` / `CrashMaxDown` | 每事件触发节点崩溃的概率 / 下线时长区间 |
| `Faults.PartitionProb` / `PartMinHold` / `PartMaxHold` | 每事件触发网络分区的概率 / 持续时长区间；分区期间跨区消息被暂扣，恢复后补投 |
| `Faults.ClockJumpProb` / `ClockJumpMax` | 每事件触发时钟跳变的概率 / 最大幅度；跳变只影响该节点 `Now()` 的读数 |

所有概率都在每次事件处理后由种子对应的随机流判定，因此"故障何时发生"
本身也是可复现的；每次注入都记录为 `FaultRecord{EventID, Time, Kind, Detail}`。

## 不变式校验

`AddInvariant(name, check)` 注册的不变式在每个事件处理后执行。
违反时返回 `InvariantError`，携带可定位的最小复现信息：
种子、事件 ID、不变式名称，以及最近 16 个关键事件的序列。

内置示例工作负载（`kv.go`）演示了三类典型不变式：
已确认的结果不能丢失、同一操作不能重复生效、可见状态必须自洽。

## 快照与恢复

```go
data, _ := e.Snapshot()                       // 任意事件边界上保存
e2, _ := sim.RestoreEngine(cfg, data, factory) // 从该点继续
```

继续推进的演化与不中断运行**逐位一致**（最终状态哈希相同，有测试保证）。
快照带显式版本号（`SnapshotVersion`）：恢复旧版本数据返回
`ErrSnapshotVersion`，数据损坏或节点恢复失败返回具体错误，
绝不悄悄读出不完整的状态。同名定时器是"替换"语义，崩溃会清掉易失定时器，
这些状态都在快照内。

## 重放

```go
cfg.RecordLog = true
cfg.CheckpointEvery = 100
// ... 运行 ...
log := e.Log()
sim.Replay(cfg, log, factory)            // 从种子重放全部事件
sim.ReplayFrom(cfg, log, 500, factory)   // 从事件 500 的检查点重放
```

重放逐事件校验状态哈希，不依赖真实时间与真实网络；
节点只通过 `Context` 动作，因此重放不会再次触发对外副作用。
日志被篡改或状态发散时，重放在第一个不一致的事件处报错。

## 终止语义

`Run` 在以下情况停止，且总是停在完整事件处理完的一致状态
（可立即快照、可继续推进），不会留下写了一半的结果：

- `exhausted`：事件队列已空；
- `max_steps` / `max_events` / `max_time`：达到配置的上限；
- `canceled`：`context` 被取消；
- `invariant`：不变式违反（返回 `InvariantError`）。

每个 `Engine` 持有独立的随机源与状态，多次运行互不污染。

## 性能

- 调度：事件堆 O(log n)；单线程无锁。
- 内存：事件日志与检查点可选（`RecordLog` / `CheckpointEvery`），
  长运行可关闭；最近事件环形缓冲固定 16 条。
- 随机源为 64 位状态递推，长时间推进不会漂移；
  50 万事件量级长跑的确定性有测试覆盖（`TestLongRun`）。

## 测试

```sh
go test ./...          # 全部测试
go test -short ./...   # 跳过长跑测试
```

覆盖：确定性（同种子逐条一致）、故障注入（八类故障全注入且不破坏不变式）、
快照恢复（续跑与不中断一致、版本不兼容拒绝、损坏数据报错）、
重放（种子重放、任意检查点重放、篡改检出）、
终止（各类上限、外部取消、不变式违反的可定位错误）、规模（200 客户端）。
