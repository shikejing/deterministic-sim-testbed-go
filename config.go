package sim

// FaultConfig 控制各类故障注入的概率与参数。
// 所有概率都在 [0,1]，是否触发由种子对应的随机流决定。
type FaultConfig struct {
	DropProb        float64 // 消息丢失概率
	DuplicateProb   float64 // 消息重复概率
	ReorderProb     float64 // 消息乱序（额外延迟）概率
	ReorderMaxDelay int64   // 乱序附加的最大额外延迟（虚拟纳秒）

	CrashProb     float64 // 每个事件步触发节点崩溃的概率
	CrashMinDown  int64   // 崩溃后最短下线时长
	CrashMaxDown  int64   // 崩溃后最长下线时长
	PartitionProb float64 // 每个事件步触发网络分区的概率
	PartMinHold   int64   // 分区最短持续时长
	PartMaxHold   int64   // 分区最长持续时长
	ClockJumpProb float64 // 每个事件步触发时钟跳变的概率
	ClockJumpMax  int64   // 时钟跳变最大幅度
}

// Config 是一次仿真运行的全部配置。
type Config struct {
	Seed uint64 // 随机种子：同一种子运行结果逐条一致

	MaxSteps  uint64 // 最大调度步数，0 表示不限
	MaxEvents uint64 // 最大事件数，0 表示不限
	MaxTime   int64  // 虚拟时间上限，0 表示不限

	MinLatency int64 // 消息基础延迟下界（虚拟纳秒）
	MaxLatency int64 // 消息基础延迟上界（虚拟纳秒）

	Faults FaultConfig

	RecordLog       bool   // 是否记录完整事件日志（用于重放）
	CheckpointEvery uint64 // 每多少事件保存一个检查点，0 表示不保存
}
