// simdemo 是确定性仿真测试台的演示程序：
// 用复制 KV 集群在注入各类故障的情况下运行，打印可复现的终态哈希，
// 并可选地做全程重放校验与快照-恢复等价性校验。
//
// 用法：
//
//	go run ./cmd/simdemo -seed 42 -nodes 5 -ops 60 -steps 2000
//	go run ./cmd/simdemo -seed 42 -replay -snapshot-check
//
// 同一组参数在任何机器、任何时刻运行，输出的终态哈希都完全相同。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/shikejing/deterministic-sim-testbed-go/sim"
)

func main() {
	var (
		seed          = flag.Uint64("seed", 1, "仿真种子：同一种子产生完全相同的运行")
		nodes         = flag.Int("nodes", 5, "KV 集群节点数")
		ops           = flag.Int("ops", 60, "客户端写操作数")
		maxSteps      = flag.Uint64("steps", 2000, "最大仿真步数")
		drop          = flag.Float64("drop", 0.1, "消息丢失概率")
		dup           = flag.Float64("dup", 0.1, "消息重复概率")
		reorder       = flag.Float64("reorder", 0.2, "消息乱序概率")
		crash         = flag.Float64("crash", 0.01, "每步节点崩溃概率")
		partition     = flag.Float64("partition", 0.01, "每步网络分区概率")
		clockJump     = flag.Float64("clock-jump", 0.01, "每步时钟跳变概率")
		replay        = flag.Bool("replay", false, "运行后从种子+事件日志重放校验")
		snapshotCheck = flag.Bool("snapshot-check", false, "运行中途快照-恢复等价性校验")
	)
	flag.Parse()

	cfg := sim.Config{
		Seed:            *seed,
		MaxSteps:        *maxSteps,
		LogCap:          -1,
		InvariantEvery:  10,
		CheckpointEvery: *maxSteps / 4,
		Faults: sim.FaultConfig{
			DropRate: *drop, DupRate: *dup, ReorderRate: *reorder, MaxReorderDelay: 5,
			CrashRate: *crash, RestartDelay: 20,
			PartitionRate: *partition, PartitionLen: 30,
			ClockJumpRate: *clockJump, MaxClockJump: 100,
		},
	}

	setup := func(s *sim.Simulator) {
		ids := make([]sim.NodeID, *nodes)
		for i := range ids {
			ids[i] = sim.NodeID(fmt.Sprintf("n%d", i))
		}
		quorum := *nodes/2 + 1
		for i, id := range ids {
			peers := make([]sim.NodeID, 0, *nodes-1)
			for j, p := range ids {
				if j != i {
					peers = append(peers, p)
				}
			}
			s.RegisterNode("kv", sim.NewKVNode(id, peers, quorum))
		}
		for i := 0; i < *ops; i++ {
			op := sim.KVOp{
				OpID:  fmt.Sprintf("op-%d", i),
				Key:   fmt.Sprintf("key-%d", i),
				Value: fmt.Sprintf("v-%d", i),
			}
			s.InjectClient(sim.Time(1+i*3), ids[i%*nodes], op)
		}
	}
	addInvariants := func(s *sim.Simulator) {
		ids := make([]sim.NodeID, *nodes)
		for i := range ids {
			ids[i] = sim.NodeID(fmt.Sprintf("n%d", i))
		}
		quorum := *nodes/2 + 1
		s.AddInvariant("confirmed-durable", sim.ConfirmedDurable(quorum, ids...))
		s.AddInvariant("no-duplicate-apply", sim.NoDuplicateApply(ids...))
		s.AddInvariant("values-consistent", sim.ValuesConsistent(ids...))
	}

	s := sim.New(cfg)
	setup(s)
	addInvariants(s)
	res := s.Run(context.Background())

	fmt.Printf("seed=%d stop=%s steps=%d events=%d time=%d\n",
		*seed, res.Reason, res.Steps, res.Events, res.Now)
	fmt.Printf("final-hash=%016x confirmed-effects=%d checkpoints=%v\n",
		res.FinalHash, len(s.Log().Effects()), res.Checkpoints)
	if res.Violation != nil {
		fmt.Printf("INVARIANT VIOLATED: %v\n", res.Violation)
		for _, e := range res.Violation.Recent {
			fmt.Printf("  %s\n", e)
		}
		os.Exit(1)
	}

	if *replay {
		_, rres, err := sim.Replay(cfg, setup, s.Log(), 0)
		if err != nil {
			fmt.Println("replay FAILED:", err)
			os.Exit(1)
		}
		fmt.Printf("replay-ok hash=%016x (match=%v)\n", rres.FinalHash, rres.FinalHash == res.FinalHash)
	}

	if *snapshotCheck {
		half := sim.New(cfg)
		half.Configure(func(c *sim.Config) { c.MaxSteps = res.Steps / 2 })
		setup(half)
		snap, err := half.Snapshot()
		if err != nil {
			fmt.Println("snapshot FAILED:", err)
			os.Exit(1)
		}
		resumed, err := sim.ResumeFrom(snap, map[string]func() sim.Node{
			"kv": func() sim.Node { return &sim.KVNode{} },
		})
		if err != nil {
			fmt.Println("resume FAILED:", err)
			os.Exit(1)
		}
		resumed.Configure(func(c *sim.Config) { c.MaxSteps = *maxSteps })
		addInvariants(resumed)
		rres := resumed.Run(context.Background())
		fmt.Printf("snapshot-resume-hash=%016x (match=%v)\n", rres.FinalHash, rres.FinalHash == res.FinalHash)
		if rres.FinalHash != res.FinalHash {
			os.Exit(1)
		}
	}
}
