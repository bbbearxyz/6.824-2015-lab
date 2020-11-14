package pbservice

import (
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	curView	   viewservice.View
	dataMap    map[string] string
	dumpMap	   map[int64] int


}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.curView.Primary != pb.me {
		reply.Err = ErrWrongPrimary
		return nil
	}
	if pb.dumpMap[args.ClerkId] >= args.ReqId {
		reply.Err = OK
		return nil
	}

	newArgs := &SendArgs{
		Type: 0,
		Host: pb.me,
		ClerkId: args.ClerkId,
		ReqId: args.ReqId,
		Op: "Get",
		Key: args.Key,
	}
	for pb.curView.Backup != "" {
		newReply := SendReply{}
		call(pb.curView.Backup, "PBServer.ReadBackupOp", newArgs, &newReply)
		if newReply.SendErr == OK {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	pb.dumpMap[args.ClerkId] = args.ReqId
	value, exist := pb.dataMap[args.Key]
	if exist != false {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
	reply.Value = value
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.curView.Primary != pb.me {
		reply.Err = ErrWrongPrimary
		return nil
	}
	if pb.dumpMap[args.ClerkId] >= args.ReqId {
		reply.Err = OK
		return nil
	}
	newArgs := &SendArgs{
		Type: 0,
		Host: pb.me,
		ClerkId: args.ClerkId,
		ReqId: args.ReqId,
		Op: args.Op,
		Key: args.Key,
		Value: args.Value,
	}
	for pb.curView.Backup != "" {
		newReply := SendReply{}
		call(pb.curView.Backup, "PBServer.ReadBackupOp", newArgs, &newReply)
		if newReply.SendErr == OK {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	if args.Op == "Put" {
		pb.dataMap[args.Key] = args.Value
		log.Printf("%s get put, key is %s, value is %s", pb.me, args.Key, args.Value)
	} else {
		pb.dataMap[args.Key] += args.Value
		log.Printf("%s get append, key is %s, value is %s", pb.me, args.Key, pb.dataMap[args.Key])
	}
	pb.dumpMap[args.ClerkId] = args.ReqId
	reply.Err = OK
	return nil
}
// 会有两种类型 一个是发送get/put/append命令 一种是发送完整的信息
type SendArgs struct {
	Type	int
	Host	string
	// 只发送get/put/append命令
	ClerkId int64
	ReqId	int
	Op 		string
	Key 	string
	Value 	string
	// 发送全部信息
	DataMap map[string] string
	DumpMap	map[int64] int

}

type SendReply struct {
	SendErr Err
}


func (pb *PBServer) ReadBackupOp(args *SendArgs, reply *SendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Host == pb.curView.Primary {
		if args.Type == 0 {
			if pb.dumpMap[args.ClerkId] >= args.ReqId {
				reply.SendErr = OK
				return nil
			}
			if args.Op == "Get" {
				// do nothing
			} else if args.Op == "Append" {
				pb.dataMap[args.Key] += args.Value
				log.Printf("%s append put, key is %s, value is %s", pb.me, args.Key, args.Value)
			} else if args.Op == "Put" {
				pb.dataMap[args.Key] = args.Value
				log.Printf("%s get put, key is %s, value is %s", pb.me, args.Key, pb.dataMap[args.Key])
			}
			pb.dumpMap[args.ClerkId] = args.ReqId
			reply.SendErr = OK
			return nil
		} else {
			pb.dataMap = args.DataMap
			pb.dumpMap = args.DumpMap
			reply.SendErr = OK
			log.Printf("%s get statemachine.", pb.me)
			return nil
		}
	} else {
		reply.SendErr = ErrWrongPrimary
		return nil
	}
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	view, _ := pb.vs.Ping(pb.curView.Viewnum)
	if view.Viewnum != pb.curView.Viewnum {
		log.Printf("view num change to %d.", view.Viewnum)
		if view.Primary == pb.me && view.Backup != "" && pb.curView.Backup != view.Backup {
			args := &SendArgs{
				Type: 1,
				Host: pb.me,
				DataMap: pb.dataMap,
				DumpMap: pb.dumpMap,
			}
			reply := SendReply{}
			log.Printf("%s send statemachine.", pb.me)
			call(view.Backup, "PBServer.ReadBackupOp", args, &reply)
			for reply.SendErr != OK {
				call(view.Backup, "PBServer.ReadBackupOp", args, &reply)
			}
		}
		pb.curView = view
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.curView = viewservice.View{
		Viewnum: 0,
	}
	pb.dataMap = make(map[string] string)
	pb.dumpMap	= make(map[int64] int)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
