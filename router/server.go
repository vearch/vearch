package router

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	limit "github.com/juju/ratelimit"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/monitor"
	"github.com/vearch/vearch/router/document"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/metrics/mserver"
	"github.com/vearch/vearch/util/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	ctx        context.Context
	httpServer *netutil.Server
	rpcServer  *grpc.Server
	cancelFunc context.CancelFunc
}

func NewServer(ctx context.Context) (*Server, error) {

	cli, err := client.NewClient(config.Conf())
	if err != nil {
		return nil, err
	}

	addr := config.LocalCastAddr

	httpServerConfig := &netutil.ServerConfig{
		Name:         "HttpServer",
		Addr:         util.BuildAddr(addr, config.Conf().Router.Port),
		ConnLimit:    config.Conf().Router.ConnLimit,
		CloseTimeout: time.Duration(config.Conf().Router.CloseTimeout),
	}
	netutil.SetMode(netutil.RouterModeGorilla) //no need
	httpServer := netutil.NewServer(httpServerConfig)
	document.ExportDocumentHandler(httpServer, cli)

	var rpcServer *grpc.Server
	if config.Conf().Router.RpcPort > 0 {
		lis, err := net.Listen("tcp", util.BuildAddr(addr, config.Conf().Router.RpcPort))
		if err != nil {
			panic(fmt.Errorf("start rpc server failed to listen: %v", err))
		}
		rpcServer = grpc.NewServer()
		// rpcServer = grpc.NewServer(grpc.UnaryInterceptor(unaryInterceptor))
		// limiter := &Limiter{limit.NewBucket(time.Millisecond, 100000)}
		// rpcServer = grpc.NewServer(grpc_middleware.WithUnaryServerChain(ratelimit.UnaryServerInterceptor(limiter)),
		// grpc_middleware.WithStreamServerChain(ratelimit.StreamServerInterceptor(limiter)))
		go func() {
			if err := rpcServer.Serve(lis); err != nil {
				panic(fmt.Errorf("start rpc server failed to start: %v", err))
			}
		}()
		document.ExportRpcHandler(rpcServer, cli)
	}

	routerCtx, routerCancel := context.WithCancel(ctx)
	// start router cache
	if err := cli.Master().FlushCacheJob(routerCtx); err != nil {
		log.Error("Error in Start cache Job,Err:%v", err)
		panic(err)
	}

	return &Server{
		httpServer: httpServer,
		ctx:        routerCtx,
		cancelFunc: routerCancel,
		rpcServer:  rpcServer,
	}, nil
}

func (server *Server) Start() error {
	//find ip for server
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			match, _ := regexp.MatchString(`^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$`, addr.String())
			if !match {
				continue
			}
			slit := strings.Split(addr.String(), "/")
			mserver.SetIp(slit[0], false)
			break
		}
	}

	if port := config.Conf().Router.MonitorPort; port > 0 {
		monitor.Register(nil, nil, config.Conf().Router.MonitorPort)
	}

	if err := server.httpServer.Run(); err != nil {
		return fmt.Errorf("Fail to start http Server, %v", err)
	}
	log.Info("router exited!")

	return nil
}

func (server *Server) Shutdown() {
	server.cancelFunc()
	log.Info("router shutdown... start")
	if server.httpServer != nil {
		server.httpServer.Shutdown()
		server.httpServer = nil
	}
	log.Info("router shutdown... end")
}

/* For GRPC */
var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	errInvalidToken    = status.Errorf(codes.Unauthenticated, "invalid token")
)

func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// authentication (token verification)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}
	if !valid(md["authorization"]) {
		return nil, errInvalidToken
	}
	m, err := handler(ctx, req)
	if err != nil {
		log.Error("RPC failed with error %v", err)
	}
	return m, err
}

// valid validates the authorization.
func valid(authorization []string) bool {
	if len(authorization) < 1 {
		return false
	}
	// username, password, err := util.AuthDecrypt(headerData)
	return true
}

type Limiter struct {
	bucker *limit.Bucket
}

func (l *Limiter) Limit() bool {
	return !l.bucker.WaitMaxDuration(1, time.Second)
}
