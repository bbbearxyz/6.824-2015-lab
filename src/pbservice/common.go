package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"

	ErrWrongPrimary = "ErrWrongPrimary"

)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	ClerkId	int64
	ReqId	int
	Op		string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId	int64
	ReqId	int
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
