package raftkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	Debug      = 0
	WaitPeriod = time.Duration(1000) * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string //Put or Append or Get
	Key    string
	Value  string
	Clerk  int64
	Index  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clerkLog  map[int64]int
	kvDB      map[string]string
	msgCh     map[int]chan int
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, "", args.ClerkID, args.CmdIndex}
	//raft.InfoKV.Printf("KVServer:%2d | receive RPC! Clerk:[%20v] index:[%4d]\n", kv.me, op.Clerk, op.Index)
	reply.Err = ErrNoKey
	reply.WrongLeader = true

	//raft.InfoKV.Printf("KVServer:%2d | Begin Method:[%s] clerk:[%20v] index:[%4d]\n", kv.me, op.Method, op.Clerk, op.Index)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		//raft.InfoKV.Printf("KVServer:%2d | Sry, I am not leader\n", kv.me)
		return
	}

	kv.mu.Lock()
	if ind, ok := kv.clerkLog[args.ClerkID]; ok && ind >= args.CmdIndex {

		//raft.InfoKV.Printf("KVServer:%2d | Cmd has been finished: Method:[%s] clerk:[%v] index:[%4d]\n", kv.me, op.Method, op.Clerk, op.Index)
		reply.Value = kv.kvDB[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	raft.InfoKV.Printf(("KVServer:%2d | leader msgIndex:%4d\n"), kv.me, index)

	ch := make(chan int)
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	select {
	case <-time.After(WaitPeriod):

		raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Timeout!\n", kv.me, index, term)
	case msgTerm := <-ch:
		if msgTerm == term {

			kv.mu.Lock()
			raft.InfoKV.Printf("KVServer:%2d | Get {index:%4d term:%4d} OK!\n", kv.me, index, term)
			if val, ok := kv.kvDB[args.Key]; ok {
				reply.Value = val
				reply.Err = OK
			}
			kv.mu.Unlock()
			reply.WrongLeader = false
		} else {
			raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Not leader any more!\n", kv.me, index, term)
		}
	}

	go func() { kv.closeCh(index) }()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value, args.ClerkID, args.CmdIndex}
	//raft.InfoKV.Printf("KVServer:%2d | receive RPC! Clerk:[%20v] index:[%4d]\n", kv.me, op.Clerk, op.Index)
	reply.Err = OK
	kv.mu.Lock()

	if ind, ok := kv.clerkLog[args.ClerkID]; ok && ind >= args.CmdIndex {

		kv.mu.Unlock()
		//raft.InfoKV.Printf("KVServer:%2d | Cmd has been finished: Method:[%s] clerk:[%v] index:[%4d]\n", kv.me, op.Method, op.Clerk, op.Index)
		reply.WrongLeader = false
		return
	}
	kv.mu.Unlock()

	//raft.InfoKV.Printf("KVServer:%2d | Begin Method:[%s] clerk:[%20v] index:[%4d]\n", kv.me, op.Method, op.Clerk, op.Index)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		//raft.InfoKV.Printf("KVServer:%2d | Sry, I am not leader\n", kv.me)
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	raft.InfoKV.Printf(("KVServer:%2d | leader msgIndex:%4d\n"), kv.me, index)
	ch := make(chan int)
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = true
	select {
	case <-time.After(WaitPeriod):

		raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} Failed, timeout!\n", kv.me, index, term)
	case msgTerm := <-ch:
		if msgTerm == term {

			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} OK!\n", kv.me, index, term)
			reply.WrongLeader = false
		} else {
			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} Failed, not leader!\n", kv.me, index, term)
		}
	}
	go func() { kv.closeCh(index) }()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	raft.InfoKV.Printf("KVServer:%2d | KV server is died!\n", kv.me)
	kv.mu.Unlock()

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.kvDB = make(map[string]string)
	kv.clerkLog = make(map[int64]int)
	kv.msgCh = make(map[int]chan int)
	kv.persister = persister

	kv.loadSnapshot()

	raft.InfoKV.Printf("KVServer:%2d | Create New KV server!\n", kv.me)

	go kv.receiveNewMsg()

	return kv
}

func (kv *KVServer) receiveNewMsg() {
	for msg := range kv.applyCh {
		kv.mu.Lock()

		index := msg.CommandIndex
		term := msg.CommitTerm
		//role := msg.Role

		if !msg.CommandValid {
			//snapshot
			op := msg.Command.([]byte)
			kv.decodedSnapshot(op)
			kv.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)

		if ind, ok := kv.clerkLog[op.Clerk]; ok && ind >= op.Index {

		} else {

			kv.clerkLog[op.Clerk] = op.Index
			switch op.Method {
			case "Put":
				kv.kvDB[op.Key] = op.Value
				//if role == raft.Leader {
				//	raft.InfoKV.Printf("KVServer:%2d | Put successful!  clerk:[%v] Cindex:[%4d] Mindex:[%4d]\n", kv.me, op.Clerk, op.Index, index)
				//}
			case "Append":
				if _, ok := kv.kvDB[op.Key]; ok {
					kv.kvDB[op.Key] = kv.kvDB[op.Key] + op.Value
				} else {
					kv.kvDB[op.Key] = op.Value
				}
				//if role == raft.Leader {
				//	raft.InfoKV.Printf("KVServer:%2d | Append successful! clerk:[%v] Cindex:[%4d] Mindex:[%4d]\n", kv.me, op.Clerk, op.Index, index)
				//}
			case "Get":
				//if role == raft.Leader {
				//	raft.InfoKV.Printf("KVServer:%2d | Get successful! clerk:[%v] Cindex:[%4d] Mindex:[%4d]\n", kv.me, op.Clerk, op.Index, index)
				//}
			}
		}

		if ch, ok := kv.msgCh[index]; ok {
			ch <- term
		}

		kv.checkState(index, term)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) closeCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.msgCh[index])
	delete(kv.msgCh, index)
}

func (kv *KVServer) decodedSnapshot(data []byte) {

	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var db map[string]string
	var cl map[int64]int

	if dec.Decode(&db) != nil || dec.Decode(&cl) != nil {
		raft.InfoKV.Printf("KVServer:%2d | KV Failed to recover by snapshot!\n", kv.me)
	} else {
		kv.kvDB = db
		kv.clerkLog = cl
		raft.InfoKV.Printf("KVServer:%2d | KV recover frome snapshot successful! \n", kv.me)
	}
}

func (kv *KVServer) checkState(index int, term int) {

	if kv.maxraftstate == -1 {
		return
	}

	portion := 2 / 3

	if kv.persister.RaftStateSize() < kv.maxraftstate*portion {
		return
	}

	rawSnapshot := kv.encodeSnapshot()
	go func() { kv.rf.TakeSnapshot(rawSnapshot, index, term) }()
}

func (kv *KVServer) encodeSnapshot() []byte {

	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.kvDB)
	enc.Encode(kv.clerkLog)
	data := w.Bytes()
	return data
}

func (kv *KVServer) loadSnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) == 0 {
		return
	}
	kv.decodedSnapshot(data)
}
