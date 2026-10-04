package sim

import "fmt"

// LogEntry 是一条事件日志记录。
type LogEntry struct {
	Index     uint64
	EventID   uint64
	Time      int64
	Kind      EventKind
	Detail    string
	StateHash uint64 // 该事件处理后的整体状态哈希
}

// Checkpoint 是带事件序号的快照。
type Checkpoint struct {
	Index uint64
	Data  []byte
}

// EventLog 记录事件流与检查点，用于重放。
// 重放只依赖本日志与种子，不依赖真实时间和真实网络。
type EventLog struct {
	Seed        uint64
	NodeIDs     []NodeID
	Entries     []LogEntry
	Checkpoints []Checkpoint
}

// Log 返回引擎记录的事件日志（未开启 RecordLog 时返回 nil）。
func (e *Engine) Log() *EventLog { return e.log }

// Replay 从种子重放整个日志，逐个事件校验状态哈希一致。
// 重放不触发任何对外副作用：节点只能通过与 Context 交互来动作。
func Replay(cfg Config, log *EventLog, factory NodeFactory) error {
	if log == nil {
		return fmt.Errorf("sim: nil event log")
	}
	cfg.RecordLog = false
	cfg.Seed = log.Seed
	e := NewEngine(cfg)
	for _, id := range log.NodeIDs {
		n := factory(id)
		if n == nil {
			return fmt.Errorf("sim: factory returned nil for node %s", id)
		}
		e.AddNode(id, n)
	}
	return replayEntries(e, log, 0)
}

// ReplayFrom 从指定检查点恢复并重放到日志末尾，校验之后每个事件的状态一致。
func ReplayFrom(cfg Config, log *EventLog, checkpointIndex uint64, factory NodeFactory) error {
	if log == nil {
		return fmt.Errorf("sim: nil event log")
	}
	var cp *Checkpoint
	for i := range log.Checkpoints {
		if log.Checkpoints[i].Index == checkpointIndex {
			cp = &log.Checkpoints[i]
			break
		}
	}
	if cp == nil {
		return fmt.Errorf("sim: no checkpoint at index %d", checkpointIndex)
	}
	cfg.RecordLog = false
	cfg.Seed = log.Seed
	e, err := RestoreEngine(cfg, cp.Data, factory)
	if err != nil {
		return fmt.Errorf("sim: replay from checkpoint %d: %w", checkpointIndex, err)
	}
	e.log = nil // 重放不再追加日志，避免污染原日志
	return replayEntries(e, log, checkpointIndex)
}

// replayEntries 从事件序号 after 开始逐步重放并校验哈希。
func replayEntries(e *Engine, log *EventLog, after uint64) error {
	for _, entry := range log.Entries {
		if entry.Index <= after {
			continue
		}
		if err := e.step(); err != nil {
			return fmt.Errorf("sim: replay event %d: %w", entry.Index, err)
		}
		if got := e.stateHash(); got != entry.StateHash {
			return fmt.Errorf("sim: replay diverged at event %d (%s): hash %x, want %x",
				entry.Index, entry.Detail, got, entry.StateHash)
		}
	}
	return nil
}
