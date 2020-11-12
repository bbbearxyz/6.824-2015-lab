package viewservice

import (
	"net"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	timeRecord	map[string] time.Time
	currentView	View
	isAcked	    bool
	nextView 	View

	// 保存idle信息
	idle		string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	fmt.Printf("%s ping. now Primary is %s, now Backup is %s.\n", args.Me, vs.currentView.Primary, vs.currentView.Backup)
	// 如果是primary和backup都存在 则ping就是idle
	if args.Viewnum == 0 && vs.currentView.Primary != "" && vs.currentView.Primary != args.Me && vs.currentView.Backup != "" && vs.currentView.Backup != args.Me {
		vs.idle = args.Me
		reply.View = vs.currentView
		return nil
	}
	// 更新最新的ping的响应
	// 可能会有bug
	vs.timeRecord[args.Me] = time.Now()
	// 初始化的时候
	if vs.currentView.Viewnum == 0 {
		vs.currentView.Viewnum = 1
		vs.currentView.Primary = args.Me
		reply.View = vs.currentView
		fmt.Printf("%s become primary\n", vs.currentView.Primary)
		vs.isAcked = false
		return nil
	}
	// primary响应
	if vs.currentView.Viewnum == args.Viewnum && vs.currentView.Primary == args.Me {
		vs.isAcked = true
	}
	// 来了一个backUp 在tick阶段就可以对backup更新
	if args.Viewnum == 0 && (vs.currentView.Primary == args.Me || vs.currentView.Primary == "" || vs.currentView.Backup == "") {
		// 如果server重连 则设定成这样
		if vs.currentView.Primary == args.Me {
			vs.nextView.Viewnum = uint(int(vs.currentView.Viewnum) + 1)
			vs.nextView.Primary = vs.currentView.Backup
			if vs.nextView.Primary != "" {
				vs.nextView.Backup = args.Me
			} else {
				vs.nextView.Primary = args.Me
			}
			vs.currentView = vs.nextView
			fmt.Printf("%s become primary\n", vs.currentView.Primary)
			vs.nextView = View{}
			vs.isAcked = false
		} else if vs.currentView.Primary == "" {
			vs.nextView.Primary = args.Me
		} else if vs.currentView.Backup == "" && vs.nextView.Primary != args.Me {
			vs.nextView.Backup = args.Me
		}
	}

	// 如果发现view可以更新 并且 有新的信息出现
	if vs.isAcked && vs.currentView.Backup == "" && vs.nextView.Backup != "" {
		vs.nextView.Viewnum = uint(int(vs.currentView.Viewnum) + 1)
		vs.nextView.Primary = vs.currentView.Primary
		vs.currentView = vs.nextView
		vs.nextView = View{}
		vs.isAcked = false
		fmt.Printf("%s become primary\n", vs.currentView.Primary)
	}
	// 最终将现在的view信息导入到reply中
	reply.View = vs.currentView
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.currentView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	timer := time.After(PingInterval)

	for {
		select {
		case <- timer:
			timer = time.After(PingInterval)
			// 判断是否该进行替换了
			// 如果确定primary已经dead了
			vs.mu.Lock()
			if vs.isAcked && vs.currentView.Primary != "" && time.Now().Sub(vs.timeRecord[vs.currentView.Primary]) > DeadPings * PingInterval {
				vs.nextView.Viewnum = uint(int(vs.currentView.Viewnum) + 1)
				vs.nextView.Primary = vs.currentView.Backup
				if vs.currentView.Backup == "" {
					vs.isAcked = true
				} else {
					vs.isAcked = false
				}
				vs.nextView.Backup = vs.idle
				vs.idle = ""
				vs.currentView = vs.nextView
				vs.nextView = View{}
				fmt.Printf("%s become primary\n", vs.currentView.Primary)
			}
			if vs.isAcked && vs.currentView.Backup != "" && time.Now().Sub(vs.timeRecord[vs.currentView.Backup]) > DeadPings * PingInterval {
				// 自动移除dead backup
				vs.nextView.Viewnum = uint(int(vs.currentView.Viewnum) + 1)
				vs.nextView.Primary = vs.currentView.Primary
				vs.isAcked = false
				vs.nextView.Backup = vs.idle
				vs.idle = ""
				vs.currentView = vs.nextView
				vs.nextView = View{}

			}
			vs.mu.Unlock()

		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{Viewnum: 0}
	vs.nextView = View{}
	vs.timeRecord = make(map[string]time.Time)
	vs.isAcked = false
	vs.idle = ""
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
