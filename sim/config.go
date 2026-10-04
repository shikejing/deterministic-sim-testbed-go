package sim

// FaultConfig 控制各类故障注入的概率与参数。
// 所有概率均为每次判定独立生效，判定顺序与随机源由仿真器统一控制。
type FaultConfig struct {
	DropRate        float64 // 单条消息丢失概率
	DupRate         float64 // 单条消息被额外复制一次的概率
	ReorderRate     float64 // 单条消息被追加随机延迟（乱序）的概率
	MaxReorderDelay Time    // 乱序追加延迟的上限（不含）

	CrashRate    float64 // 每一步触发某节点崩溃的概率
	RestartDelay Time    // 崩溃后多久重启

	PartitionRate float64 // 每一步触发一次随机分区的概率
	PartitionLen  Time    // 分区持续时长

	ClockJumpRate float64 // 每一步触发时钟跳变的概率
	MaxClockJump  Time    // 时钟跳变幅度上限（不含）
}

// Config 是一次仿真运行的全部配置。
type Config struct {
	Seed uint64

	MaxSteps  uint64 // 最大执行步数（0 = 不限）
	MaxEvents uint64 // 最大事件创建数（0 = 不限）
	MaxTime   Time   // 虚拟时间上限（0 = 不限）

	// MaxInFlight 限制事件队列中同时在途的事件数（0 = 不限）。
	// 这是仿真“并行度”的旋钮：在途事件即系统中并发未决的工作。
	// 超出上限的新事件被丢弃并计数（见 Result.DroppedBackpressure），
	// 既限制内存，也模拟背压。判定完全确定，不影响可复现性。
	MaxInFlight int

	Faults FaultConfig

	InvariantEvery  uint64 // 每隔多少步校验一次不变式（0 = 每步都校验）
	CheckpointEvery uint64 // 每隔多少步保存一个检查点（0 = 不保存）
	MaxCheckpoints  int    // 内存中最多保留的检查点数，超出淘汰最旧的

	LogCap int // 事件日志在内存中的最大条数（0 = 不记录；<0 = 不限）
}

// withDefaults 填充派生默认值，不改变显式配置。
func (c Config) withDefaults() Config {
	if c.CheckpointEvery > 0 && c.MaxCheckpoints == 0 {
		c.MaxCheckpoints = 8
	}
	return c
}
