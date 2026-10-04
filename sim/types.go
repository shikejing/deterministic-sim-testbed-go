package sim

// Time 是虚拟时钟的滴答数，由仿真器单调推进。
type Time int64

// NodeID 标识一个仿真节点。
type NodeID string

// EventKind 区分事件的来源与语义。
type EventKind int

const (
	KindTimer     EventKind = iota // 节点设置的定时器到期
	KindDeliver                    // 网络消息送达
	KindCrash                      // 节点崩溃（故障注入）
	KindRestart                    // 节点重启（故障注入）
	KindPartition                  // 网络分区（故障注入）
	KindHeal                       // 分区恢复（故障注入）
	KindClockJump                  // 时钟跳变（故障注入）
	KindClient                     // 外部客户端请求注入
)

func (k EventKind) String() string {
	switch k {
	case KindTimer:
		return "Timer"
	case KindDeliver:
		return "Deliver"
	case KindCrash:
		return "Crash"
	case KindRestart:
		return "Restart"
	case KindPartition:
		return "Partition"
	case KindHeal:
		return "Heal"
	case KindClockJump:
		return "ClockJump"
	case KindClient:
		return "Client"
	}
	return "Unknown"
}

// Message 是节点之间通过网络发送的消息。
type Message struct {
	Kind string // 应用层消息类型
	Body any    // 应用层载荷，需可 gob 编码（snapshot 用）
}

// Event 是仿真中的一个调度单元。
type Event struct {
	ID      uint64    // 全局唯一、按创建顺序递增
	Time    Time      // 计划执行的虚拟时间
	Kind    EventKind //
	Src     NodeID    // 消息发送方 / 故障目标相关
	Dst     NodeID    // 消息接收方 / 故障目标
	Msg     Message   // KindDeliver 时有效
	Payload any       // KindTimer / KindClient 的应用层载荷
	Fault   string    // 若非空，标明该事件由哪个故障注入点产生
}

// StopReason 说明仿真为何停止。
type StopReason int

const (
	ReasonQueueEmpty StopReason = iota // 事件耗尽，自然结束
	ReasonMaxSteps                     // 达到步数上限
	ReasonMaxEvents                    // 达到事件总数上限
	ReasonMaxTime                      // 达到虚拟时间上限
	ReasonCanceled                     // 外部取消
	ReasonInvariant                    // 不变式被违反
)

func (r StopReason) String() string {
	switch r {
	case ReasonQueueEmpty:
		return "queue-empty"
	case ReasonMaxSteps:
		return "max-steps"
	case ReasonMaxEvents:
		return "max-events"
	case ReasonMaxTime:
		return "max-time"
	case ReasonCanceled:
		return "canceled"
	case ReasonInvariant:
		return "invariant-violated"
	}
	return "unknown"
}

// Result 是一次运行的最终汇报。
type Result struct {
	Reason      StopReason
	Steps       uint64 // 已执行的步数（一步 = 处理一个事件）
	Events      uint64 // 已创建的事件总数
	Now         Time
	FinalHash   uint64 // 终态哈希，用于比对两次运行是否一致
	Violation   *Violation
	Checkpoints []uint64 // 运行期间保存的检查点步号
	// DroppedBackpressure 是因 MaxInFlight 上限被丢弃的事件数。
	DroppedBackpressure uint64
}
