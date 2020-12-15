package server

import (
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
	"time"
)

var ClientOption = client.Option{
	Retries:        3,
	RPCPath:        share.DefaultRPCPath,
	ConnectTimeout: 10 * time.Second,
	SerializeType:  protocol.ProtoBuffer,
	CompressType:   protocol.None,
	BackupLatency:  10 * time.Millisecond,
}
