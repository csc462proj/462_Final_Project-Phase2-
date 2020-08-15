package shardkv

// import "shardmaster"
import (
	"bytes"
	"sync"
	"time"

	"../labrpc"

	"../shardmaster"

	"../labgob"
	"../raft"
)

const (
	putOp    = "put"
	appendOp = "append"
	getOp    = "get"

	cmdOk            = true
	cmdFail          = false
	request          = "request" //clerk request
	newConfig        = "newConfig"
	newShard         = "newShard"
	newSend          = "newsend"
	newLeader        = "newLeader"
	Cfg_Get_Time     = time.Duration(88) * time.Millisecond
	Send_Shard_Wait  = time.Duration(30) * time.Millisecond
	RPC_CALL_TIMEOUT = time.Duration(500) * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string //request or newConfig or newShard
	Shard int

	Operation string //Put or Append or Get
	Key       string
	Value     string
	Clerk     int64
	CmdIndex  int

	NewConfig NewConfig
	NewShards NewShards
}

type NewShards struct {
	ShardDB  map[string]string
	ClerkLog map[int64]int
}

type NewConfig struct {
	Cfg shardmaster.Config
}

type msgInCh struct {
	isOk bool
	op   Op
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	msgCh     map[int]chan msgInCh
	sClerkLog map[int]map[int64]int     //shard -> clerkLog
	skvDB     map[int]map[string]string //shard -> kvDB
	sm        *shardmaster.Clerk
	shards    map[int]struct{}

	shardToSend []int

	cfg    shardmaster.Config
	leader bool

	persister *raft.Persister

	waitUntilMoveDone int

	exitCh chan struct{}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	wrongLeader, wrongGroup := kv.executeOp(Op{request,
		args.Shard,
		getOp,
		args.Key,
		"",
		args.Clerk,
		args.CmdIndex,
		NewConfig{},
		NewShards{}})
	reply.WrongLeader = wrongLeader
	if wrongGroup {
		reply.Err = ErrWrongGroup
		return
	}
	if wrongLeader {
		return
	}
	kv.mu.Lock()
	if val, ok := kv.skvDB[args.Shard][args.Key]; ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op string
	if args.Op == "Put" {
		op = putOp
	} else {
		op = appendOp
	}
	wrongLeader, wrongGroup := kv.executeOp(Op{request,
		args.Shard,
		op,
		args.Key,
		args.Value,
		args.Clerk,
		args.CmdIndex,
		NewConfig{},
		NewShards{}})
	reply.WrongLeader = wrongLeader
	reply.Err = OK
	if wrongGroup {
		reply.Err = ErrWrongGroup
	}
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	//raft.ShardInfo.Printf("GID:%2d me:%2d leader:%6v ============>prepare to died<===========!\n", kv.gid, kv.me, kv.leader)
	kv.rf.Kill()
	kv.exitCh <- struct{}{}
	kv.exitCh <- struct{}{}
	// Your code here, if desired.
	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v <- I am died!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.msgCh = make(map[int]chan msgInCh)
	kv.sClerkLog = make(map[int]map[int64]int)
	kv.skvDB = make(map[int]map[string]string)
	kv.shards = make(map[int]struct{})

	kv.cfg = shardmaster.Config{0, [10]int{}, make(map[int][]string)}

	kv.shardToSend = make([]int, 0)

	kv.persister = persister

	kv.leader = false
	kv.exitCh = make(chan struct{}, 2)

	kv.waitUntilMoveDone = 0

	raft.ShardInfo.Printf("GID:%2d me:%2d -> Create a new server!\n", kv.gid, kv.me)

	kv.loadSnapshot()
	if kv.persister.SnapshotSize() != 0 {
		kv.parseCfg()
	}

	go kv.checkCfg()
	go kv.run()

	return kv
}

func (kv *ShardKV) run() {
	for {
		select {
		case <-kv.exitCh:
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v <- close run!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader)
			return
		case msg := <-kv.applyCh:
			kv.mu.Lock()

			index := msg.CommandIndex
			term := msg.CommitTerm

			if !msg.CommandValid {
				//snapshot
				op := msg.Command.([]byte)
				kv.decodedSnapshot(op)

				kv.parseCfg()
				kv.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)

			msgToChan := cmdOk
			switch op.Type {
			case newSend:
				if !kv.haveShard(op.Shard) && kv.shardInHand(op.Shard) {
					delete(kv.skvDB, op.Shard)
					delete(kv.sClerkLog, op.Shard)
					kv.waitUntilMoveDone--
				}
			case newConfig:

				nc := op.NewConfig

				if nc.Cfg.Num > kv.cfg.Num {

					kv.cfg = nc.Cfg
					kv.parseCfg()

					if nc.Cfg.Num == 1 {
						kv.waitUntilMoveDone = 0
						raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v|Default initialize\n", kv.gid, kv.me, kv.cfg.Num, kv.leader)
						for shard, _ := range kv.shards {
							kv.skvDB[shard] = make(map[string]string)
							kv.sClerkLog[shard] = make(map[int64]int)
						}
					}
				}
			case newShard:
				if !kv.haveShard(op.Shard) {
					msgToChan = cmdFail
				} else if !kv.shardInHand(op.Shard) {

					ns := op.NewShards
					kv.skvDB[op.Shard] = make(map[string]string)
					kv.sClerkLog[op.Shard] = make(map[int64]int)
					kv.copyDB(ns.ShardDB, kv.skvDB[op.Shard])
					kv.copyCL(ns.ClerkLog, kv.sClerkLog[op.Shard])
					kv.waitUntilMoveDone--
				}
			case request:

				if ind, ok := kv.sClerkLog[op.Shard][op.Clerk]; ok && ind >= op.CmdIndex {

					raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v|op{%v}have done! index:%4d \n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)
				} else if !kv.haveShard(op.Shard) || !kv.shardInHand(op.Shard) {

					raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v|op{%v}no responsible! index:%4d \n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)

					msgToChan = cmdFail
				} else {

					raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v|apply op{%v} index:%4d\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)

					kv.sClerkLog[op.Shard][op.Clerk] = op.CmdIndex
					switch op.Operation {
					case putOp:
						kv.skvDB[op.Shard][op.Key] = op.Value
					case appendOp:
						if _, ok := kv.skvDB[op.Shard][op.Key]; ok {
							kv.skvDB[op.Shard][op.Key] = kv.skvDB[op.Shard][op.Key] + op.Value
						} else {
							kv.skvDB[op.Shard][op.Key] = op.Value
						}
					case getOp:
					}
				}
			case newLeader:
				if kv.leader {
					raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| I am new leader!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader)
				}
			}
			if ch, ok := kv.msgCh[index]; ok {
				ch <- msgInCh{msgToChan, op}
			} else if kv.leader {
				raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} no channel index:%2d!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)
			}

			kv.checkState(index, term)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) equal(begin, done Op) bool {

	equal := begin.Type == done.Type && begin.Shard == done.Shard && begin.Clerk == done.Clerk && begin.CmdIndex == done.CmdIndex
	if equal {
		return true
	}
	return false
}

func (kv *ShardKV) haveShard(shard int) bool {

	_, ok := kv.shards[shard]
	return ok
}

func (kv *ShardKV) shardInHand(shard int) bool {

	_, ok := kv.skvDB[shard]
	return ok
}

func (kv *ShardKV) needShard() (res []int) {
	res = make([]int, 0)
	for shard, _ := range kv.shards {
		if _, ok := kv.skvDB[shard]; !ok {
			res = append(res, shard)
		}
	}
	return
}

func (kv *ShardKV) checkLeader() bool {

	_, isleader := kv.rf.GetState()
	return isleader
}

func (kv *ShardKV) convertToLeader(isleader bool) {

	oldleader := kv.leader

	kv.leader = isleader

	if kv.leader != oldleader && kv.leader {

		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d | follower turn to leader!\n", kv.gid, kv.me, kv.cfg.Num)

		kv.mu.Unlock()
		op := Op{newLeader, 1, "", "", "", -1, kv.me, NewConfig{}, NewShards{}}
		wl, wg := kv.executeOp(op)
		kv.mu.Lock()
		if wl || wg {

			kv.leader = false
			return
		}

		kv.parseCfg()

		kv.broadShard()

	}
}

func (kv *ShardKV) copyDB(src map[string]string, des map[string]string) {
	for k, v := range src {
		des[k] = v
	}
}

func (kv *ShardKV) copyCL(src map[int64]int, des map[int64]int) {
	for k, v := range src {
		des[k] = v
	}
}

func (kv *ShardKV) copyShard(src map[int]struct{}) (des map[int]struct{}) {
	des = make(map[int]struct{})
	for k, v := range src {
		des[k] = v
	}
	return
}

func (kv *ShardKV) closeCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.msgCh[index])
	delete(kv.msgCh, index)
}

func (kv *ShardKV) executeOp(op Op) (wrongLeader, wrongGroup bool) {

	index := 0
	isleader := false
	isOp := op.Type == request
	wrongLeader = true
	wrongGroup = true

	if isOp {
		kv.mu.Lock()

		if kv.leader {
			if !kv.haveShard(op.Shard) || !kv.shardInHand(op.Shard) {
				raft.ShardInfo.Printf("GID:%2d me%2d cfg:%2d leader:%6v| Do not responsible for this shard %2d\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op.Shard)
				raft.ShardInfo.Printf("responsible:{%v} need{%v}\n", kv.shards, kv.needShard())

				raft.ShardInfo.Printf("cfg:%d ==> %v\n\n", kv.cfg.Num, kv.cfg.Shards)
				kv.mu.Unlock()
				return
			}
			if ind, ok := kv.sClerkLog[op.Shard][op.Clerk]; ok && ind >= op.CmdIndex {

				raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| Command{%v} have done before\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op)
				wrongLeader, wrongGroup = false, false
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
	}
	wrongGroup = false

	index, _, isleader = kv.rf.Start(op)

	kv.mu.Lock()
	kv.convertToLeader(isleader)
	if !isleader {

		kv.mu.Unlock()
		return
	}
	wrongLeader = false

	ch := make(chan msgInCh, 1)
	kv.msgCh[index] = ch
	kv.mu.Unlock()
	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} begin! index:%2d\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)

	select {
	case <-time.After(RPC_CALL_TIMEOUT):
		wrongLeader = true
		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} index:%2d timeout!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)
	case res := <-ch:
		if res.isOk == cmdFail {

			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| exclude command{%v} index:%2d!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)
			wrongGroup = true
		} else if !kv.equal(res.op, op) {
			wrongLeader = true
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} index:%2d different between post and apply!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)
		} else {
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} index:%2d Done!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, op, index)
		}
	}

	go kv.closeCh(index)
	return
}

func (kv *ShardKV) parseCfg() {

	shards := make(map[int]struct{})

	newAdd := make(map[int]struct{})

	for shard, gid := range kv.cfg.Shards {
		if gid == kv.gid {
			shards[shard] = struct{}{}
			if !kv.shardInHand(shard) {

				newAdd[shard] = struct{}{}
			}
		}
	}

	kv.shards = shards
	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| New shards:%v\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, kv.shards)

	shardToSend := make([]int, 0)
	for shard, _ := range kv.skvDB {
		if !kv.haveShard(shard) {
			shardToSend = append(shardToSend, shard)
		}
	}
	kv.shardToSend = shardToSend
	kv.waitUntilMoveDone = len(shardToSend) + len(newAdd)

	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| total receive/send %d newAdd:{%v} | New shardToSend:%v\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, kv.waitUntilMoveDone, newAdd, kv.shardToSend)
}

func (kv *ShardKV) getCfg() {

	var cfg shardmaster.Config

	cfg = kv.sm.Query(kv.cfg.Num + 1)
	for {
		if !kv.leader || cfg.Num <= kv.cfg.Num {

			return
		}

		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| Update config to %d\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, cfg.Num)

		op := Op{
			newConfig,
			1,
			"",
			"",
			"",
			1,
			1,
			NewConfig{cfg},
			NewShards{}}

		kv.mu.Unlock()

		wl, wg := kv.executeOp(op)
		kv.mu.Lock()

		if !wl && !wg {
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:  true| Update to cfg:%2d successful!\n", kv.gid, kv.me, kv.cfg.Num, kv.cfg.Num)

			kv.broadShard()
			return
		}

	}
}

func (kv *ShardKV) checkCfg() {

	for {
		select {
		case <-kv.exitCh:
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v <- close checkCfg!\n", kv.gid, kv.me, kv.cfg.Num, kv.leader)
			return
		default:
			isleader := kv.checkLeader()
			kv.mu.Lock()
			kv.convertToLeader(isleader)
			if kv.leader && kv.waitUntilMoveDone == 0 {
				kv.getCfg()
			}
			kv.mu.Unlock()
			time.Sleep(Cfg_Get_Time)
		}
	}
}

type PushShardArgs struct {
	CfgNum   int
	Shard    int
	ShardDB  map[string]string
	ClerkLog map[int64]int
	GID      int
}

type PushShardReply struct {
	WrongLeader bool
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {

	kv.mu.Lock()

	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| receive shard %2d from gid:%2d cfg:%2d need{%v} args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, args.Shard, args.GID, args.CfgNum, kv.needShard(), args)

	if kv.leader && args.CfgNum < kv.cfg.Num {

		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	if !kv.leader || args.CfgNum > kv.cfg.Num || !kv.haveShard(args.Shard) {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	if kv.shardInHand(args.Shard) {

		kv.mu.Unlock()
		reply.WrongLeader = false
		return
	}

	kv.mu.Unlock()

	op := Op{newShard,
		args.Shard,
		"",
		"",
		"",
		-1,
		-1,
		NewConfig{},
		NewShards{args.ShardDB, args.ClerkLog}}

	wrongLeader, wrongGroup := kv.executeOp(op)
	reply.WrongLeader = true
	if !wrongLeader && !wrongGroup {

		reply.WrongLeader = false
		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| add new shard %d done! \n", kv.gid, kv.me, kv.cfg.Num, kv.leader, args.Shard)
	}

	return

}

func (kv *ShardKV) broadShard() {

	for _, shard := range kv.shardToSend {

		go kv.sendShard(shard, kv.cfg.Num)
	}
	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v|Begin to transfer {%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, kv.shardToSend)
}

func (kv *ShardKV) sendShard(shard int, cfg int) {

	for {
		kv.mu.Lock()

		if !kv.leader || !kv.shardInHand(shard) || kv.cfg.Num > cfg {
			kv.mu.Unlock()
			return
		}

		gid := kv.cfg.Shards[shard]
		kvDB := make(map[string]string)
		ckLog := make(map[int64]int)
		kv.copyDB(kv.skvDB[shard], kvDB)
		kv.copyCL(kv.sClerkLog[shard], ckLog)

		args := PushShardArgs{cfg, shard, kvDB, ckLog, kv.gid}

		if servers, ok := kv.cfg.Groups[gid]; ok {

			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| transfer shard %2d to gid:%2d args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, shard, gid, args)
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply PushShardReply

				kv.mu.Unlock()

				ok := srv.Call("ShardKV.PushShard", &args, &reply)

				kv.mu.Lock()

				if !kv.leader {
					kv.mu.Unlock()
					return
				}

				if ok && reply.WrongLeader == false {

					raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| OK! transfer shard %2d to gid:%2d args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, shard, gid, args)
					kv.mu.Unlock()

					op := Op{newSend,
						shard,
						"",
						"",
						"",
						-1,
						-1,
						NewConfig{},
						NewShards{}}
					wl, wg := kv.executeOp(op)

					if wl {

						return
					}

					if !wl && !wg {

						return
					}
					kv.mu.Lock()

					break
				}
			}
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| Fail to transfer shard %2d to gid:%2d args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.leader, shard, gid, args)
		}

		kv.mu.Unlock()

		time.Sleep(Send_Shard_Wait)
	}
}

func (kv *ShardKV) decodedSnapshot(data []byte) {

	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var db map[int]map[string]string
	var cl map[int]map[int64]int
	var cfg shardmaster.Config

	if dec.Decode(&db) != nil || dec.Decode(&cl) != nil || dec.Decode(&cfg) != nil {
		raft.ShardInfo.Printf("GID:%2d me:%2d leader:%6v| KV Failed to recover by snapshot!\n", kv.gid, kv.me, kv.leader)
	} else {
		kv.skvDB = db
		kv.sClerkLog = cl
		kv.cfg = cfg
		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| KV recover from snapshot successful! \n", kv.gid, kv.me, kv.cfg.Num, kv.leader)
	}
}

func (kv *ShardKV) checkState(index int, term int) {

	if kv.maxraftstate == -1 {
		return
	}

	portion := 3 / 4

	if kv.persister.RaftStateSize() < kv.maxraftstate*portion {
		return
	}

	rawSnapshot := kv.encodeSnapshot()
	go func() { kv.rf.TakeSnapshot(rawSnapshot, index, term) }()
}

func (kv *ShardKV) encodeSnapshot() []byte {

	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.skvDB)
	enc.Encode(kv.sClerkLog)
	enc.Encode(kv.cfg)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) loadSnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) == 0 {
		return
	}
	kv.decodedSnapshot(data)
}
