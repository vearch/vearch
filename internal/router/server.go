// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package router

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"github.com/vearch/vearch/v3/internal/pkg/netutil"
	"github.com/vearch/vearch/v3/internal/router/document"
	"google.golang.org/grpc"
)

// Server represents the router server that handles HTTP and RPC requests.
// It manages the lifecycle of both HTTP and gRPC servers, handles authentication,
// and routes requests to appropriate partition servers.
type Server struct {
	ctx        context.Context
	cli        *client.Client
	httpServer *http.Server
	ginEngine  *gin.Engine
	rpcServer  *grpc.Server
	cancelFunc context.CancelFunc
	errChan    chan error
}

// NewServer creates and initializes a new router server instance.
// It registers the router with the master, sets up HTTP and optionally RPC servers,
// configures middleware, and starts background jobs.
//
// Parameters:
//   - ctx: The parent context for server lifecycle management
//
// Returns:
//   - *Server: The initialized server instance
//   - error: Any error encountered during initialization
func NewServer(ctx context.Context) (*Server, error) {
	// Validate configuration
	if err := validateConfig(config.Conf()); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	cli, err := client.NewClient(config.Conf())
	if err != nil {
		return nil, err
	}

	res, err := cli.Master().RegisterRouter(ctx, config.Conf().Global.Name, time.Duration(10*time.Second))
	if err != nil {
		return nil, err
	}

	log.Info("register router success, res: %s", res)

	addr := config.LocalCastAddr

	gin.SetMode(gin.ReleaseMode)
	httpServer := gin.New()
	httpServer.Use(func(c *gin.Context) {
		rid := c.GetHeader("X-Request-Id")
		if rid == "" {
			id, err := uuid.NewRandom()
			if err != nil {
				log.Error("generate request id failed, %v", err)
				c.Next()
				return
			}
			rid = id.String()
			rid = base64.StdEncoding.EncodeToString([]byte(rid))[:10]
			c.Request.Header.Add("X-Request-Id", rid)
		}

		// set request id to context
		c.Header("X-Request-Id", rid)
		c.Next()
	})
	if len(config.Conf().Router.AllowOrigins) > 0 {
		corsConfig := cors.DefaultConfig()
		corsConfig.AllowCredentials = true
		corsConfig.AllowOrigins = config.Conf().Router.AllowOrigins
		log.Info("use cors, AllowOrigins: %v", corsConfig.AllowOrigins)
		httpServer.Use(cors.New(corsConfig))
	}

	document.ExportDocumentHandler(httpServer, cli)

	errChan := make(chan error, 1)

	var rpcServer *grpc.Server
	if config.Conf().Router.RpcPort > 0 {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, config.Conf().Router.RpcPort))
		if err != nil {
			return nil, fmt.Errorf("start rpc server failed to listen: %v", err)
		}
		rpcServer = grpc.NewServer()
		go func() {
			log.Info("Starting RPC server on port %d", config.Conf().Router.RpcPort)
			if err := rpcServer.Serve(lis); err != nil {
				log.Error("RPC server failed: %v", err)
				select {
				case errChan <- fmt.Errorf("rpc server error: %v", err):
				default:
				}
			}
		}()
		// document.ExportRpcHandler(rpcServer, cli)
	}

	routerCtx, routerCancel := context.WithCancel(ctx)
	// start router cache
	if err := cli.Master().FlushCacheJob(routerCtx); err != nil {
		log.Error("Failed to start cache job: %v", err)
		return nil, fmt.Errorf("failed to start cache job: %w", err)
	}

	return &Server{
		ginEngine:  httpServer,
		httpServer: nil, // Will be set in Start()
		ctx:        routerCtx,
		cli:        cli,
		cancelFunc: routerCancel,
		rpcServer:  rpcServer,
		errChan:    errChan,
	}, nil
}

// Start begins serving HTTP requests and starts the heartbeat job.
// This method blocks until the HTTP server is stopped.
// It retrieves the local IP, starts monitoring if configured, and runs the HTTP server.
//
// Returns:
//   - error: Any error that occurs during server startup or operation
func (server *Server) Start() error {
	var routerIP string
	var err error
	// get local IP addr
	routerIP, err = netutil.GetLocalIP()
	if err != nil {
		log.Error("Failed to get local IP: %v", err)
		return fmt.Errorf("failed to get local IP: %w", err)
	}
	log.Debugf("Get router ip: [%s]", routerIP)
	mserver.SetIp(routerIP, false)

	server.StartHeartbeatJob(routerIP)

	if port := config.Conf().Router.MonitorPort; port > 0 {
		monitor.Register(nil, nil, config.Conf().Router.MonitorPort)
	}

	// Create HTTP server with graceful shutdown support
	server.httpServer = &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", config.Conf().Router.Port),
		Handler: server.ginEngine,
	}

	log.Info("Starting HTTP server on port %d", config.Conf().Router.Port)
	if err := server.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("fail to start http Server, %v", err)
	}
	log.Info("router exited!")

	return nil
}

// Shutdown gracefully stops the router server.
// It stops accepting new requests, gracefully shuts down HTTP server with timeout,
// stops RPC server, and closes error channel.
// The shutdown process has a 30-second timeout for both HTTP and RPC servers.
func (server *Server) Shutdown() {
	log.Info("router shutdown... start")

	// 1. 停止接收新请求
	server.cancelFunc()

	// 2. 优雅关闭HTTP服务器
	if server.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.httpServer.Shutdown(ctx); err != nil {
			log.Error("HTTP server shutdown error: %v", err)
		} else {
			log.Info("HTTP server stopped gracefully")
		}
	}

	// 3. 优雅关闭RPC服务器
	if server.rpcServer != nil {
		done := make(chan struct{})
		go func() {
			server.rpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
			log.Info("RPC server stopped gracefully")
		case <-time.After(30 * time.Second):
			server.rpcServer.Stop()
			log.Warn("RPC server force stopped after timeout")
		}
	}

	// 4. 关闭错误channel
	if server.errChan != nil {
		close(server.errChan)
	}

	log.Info("router shutdown... end")
}

// GetErrorChan returns a read-only channel for monitoring server errors.
// Errors from background goroutines (like RPC server) are sent to this channel.
//
// Returns:
//   - <-chan error: A receive-only error channel
func (server *Server) GetErrorChan() <-chan error {
	return server.errChan
}

// validateConfig validates router configuration parameters.
// It checks port numbers, timeouts, and other critical settings.
//
// Parameters:
//   - cfg: The configuration to validate
//
// Returns:
//   - error: Validation error if any parameter is invalid, nil otherwise
func validateConfig(cfg *config.Config) error {
	// Validate port numbers
	if cfg.Router.Port <= 0 || cfg.Router.Port > 65535 {
		return fmt.Errorf("invalid HTTP port: %d (must be 1-65535)", cfg.Router.Port)
	}

	if cfg.Router.RpcPort < 0 || cfg.Router.RpcPort > 65535 {
		return fmt.Errorf("invalid RPC port: %d (must be 0-65535, 0 to disable)", cfg.Router.RpcPort)
	}

	if cfg.Router.MonitorPort < 0 || cfg.Router.MonitorPort > 65535 {
		return fmt.Errorf("invalid monitor port: %d (must be 0-65535, 0 to disable)", cfg.Router.MonitorPort)
	}

	// Validate timeouts
	if cfg.Router.RpcTimeOut < 0 {
		return fmt.Errorf("invalid RPC timeout: %d (must be >= 0)", cfg.Router.RpcTimeOut)
	}

	// Validate router name
	if cfg.Global.Name == "" {
		return fmt.Errorf("router name cannot be empty")
	}

	return nil
}
