// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	jaeger "github.com/uber/jaeger-client-go"
	jaegerConfig "github.com/uber/jaeger-client-go/config"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/debugutil"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/master"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"github.com/vearch/vearch/v3/internal/pkg/signals"
	"github.com/vearch/vearch/v3/internal/pkg/vearchlog"
	"github.com/vearch/vearch/v3/internal/ps"
	"github.com/vearch/vearch/v3/internal/router"
)

var (
	BuildVersion = "0.0"
	BuildTime    = "0"
	CommitID     = "xxxxx"
	confPath     string
	masterName   string
)

func init() {
	flag.StringVar(&confPath, "conf", getDefaultConfigFile(), "vearch config path")
	flag.StringVar(&masterName, "master", "", "vearch config for master name, is on local start two master must use it")
}

const (
	psTag               = "ps"
	masterTag           = "master"
	routerTag           = "router"
	allTag              = "all"
	DefaultResourceName = "default"
)

// initJaeger returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func initJaeger(service string, c *config.TracerCfg) io.Closer {
	cfg := &jaegerConfig.Configuration{
		ServiceName: service,
		Sampler: &jaegerConfig.SamplerConfig{
			Type:  c.SampleType,
			Param: c.SampleParam,
		},
		Reporter: &jaegerConfig.ReporterConfig{
			LocalAgentHostPort:         c.Host,
			LogSpans:                   false,
			DisableAttemptReconnecting: false,
			AttemptReconnectInterval:   1 * time.Minute,
		},
	}
	closer, err := cfg.InitGlobalTracer(service, jaegerConfig.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return closer
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	config.SetConfigVersion(BuildVersion, BuildTime, CommitID)

	flag.Parse()

	if confPath == "" {
		log.Error("can not get the config file, then exit the program!")
		os.Exit(1)
	}

	config.InitConfig(confPath)

	if config.Conf().Global.ResourceName == "" {
		config.Conf().Global.ResourceName = DefaultResourceName
	}

	if config.Conf().TracerCfg != nil {
		closer := initJaeger(config.Conf().Global.Name, config.Conf().TracerCfg)
		defer closer.Close()
	}
	args := flag.Args()
	if len(args) == 0 {
		args = []string{allTag}
	}

	tags := map[string]bool{allTag: false, psTag: false, routerTag: false, masterTag: false}

	for _, a := range args {
		if _, ok := tags[a]; !ok {
			log.Error("not found tags: %s it only support [ps, router, master or all]", a)
			os.Exit(1)
		}
		tags[a] = true
	}

	logName := strings.ToUpper(strings.Join(args, "-"))
	vearchlog.SetConfig(config.Conf().GetLogFileNum(), 1024*1024*config.Conf().GetLogFileSize())
	log.Regist(vearchlog.NewVearchLog(config.Conf().GetLogDir(), logName, config.Conf().GetLevel(), false))

	log.Info("start server by version:[%s] commitID:[%s]", BuildVersion, CommitID)
	log.Info("config file: %v", confPath)

	entity.SetPrefixAndSequence(config.Conf().Global.Name)
	log.Info("The cluster prefix is: %v", entity.PrefixEtcdClusterID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if log.IsDebugEnabled() {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if config.LogInfoPrintSwitch {
					var mem runtime.MemStats
					runtime.ReadMemStats(&mem)
					log.Debug("mem.Alloc:", mem.Alloc, " mem.TotalAlloc:", mem.TotalAlloc, " mem.HeapAlloc:", mem.HeapAlloc, " mem.HeapSys:", mem.HeapSys, " routing :", runtime.NumGoroutine())
				}
				time.Sleep(3 * time.Minute)
			}
		}()
	}

	sigsHook := signals.NewSignalHook()

	var paths = make(map[string]bool)
	paths[config.Conf().GetDataDir()] = true
	paths[config.Conf().GetLogDir()] = true
	var models []string
	// start master
	if tags[masterTag] || tags[allTag] {
		if err := config.Conf().CurrentByMasterNameDomainIp(masterName); err != nil {
			log.Error("CurrentByMasterNameDomainIp master error: %v", err)
			os.Exit(1)
		}

		if err := config.Conf().Validate(config.Master); err != nil {
			log.Error("validate master error: %v", err)
			os.Exit(1)
		}

		self := config.Conf().Masters.Self()
		mserver.SetIp(self.Address, true)
		models = append(models, "master")

		s, err := master.NewServer(ctx)
		if err != nil {
			log.Error("new master error: %v", err)
			os.Exit(1)
		}
		sigsHook.AddSignalHook(func() {
			s.Stop()
		})
		go func() {
			if err := s.Start(); err != nil {
				log.Error("start master error: %v", err)
				os.Exit(1)
			}
		}()

		if port := config.Conf().Masters.Self().PprofPort; port > 0 {
			debugutil.StartUIPprofListener(int(port))
		}
	}

	// start ps
	if tags[psTag] || tags[allTag] {
		if err := config.Conf().Validate(config.PS); err != nil {
			log.Error("validate ps error: %v", err)
			os.Exit(1)
		}

		server := ps.NewServer(ctx)

		models = append(models, "ps")
		sigsHook.AddSignalHook(func() {
			if server != nil {
				err := server.Close()
				if err != nil {
					log.Error("close ps error: %v", err)
				}
			}
		})
		go func() {
			if err := server.Start(); err != nil {
				log.Error("start ps error: %v", err)
				os.Exit(1)
			}
		}()

		if port := config.Conf().PS.PprofPort; port > 0 {
			debugutil.StartUIPprofListener(int(port))
		}
	}

	// start router
	if tags[routerTag] || tags[allTag] {
		if err := config.Conf().Validate(config.Router); err != nil {
			log.Error("validate router error: %v", err)
			os.Exit(1)
		}
		server, err := router.NewServer(ctx)
		if err != nil {
			log.Error("new router error: %v", err)
			os.Exit(1)
		}
		models = append(models, "router")
		sigsHook.AddSignalHook(func() {
			cancel()
			server.Shutdown()
		})
		go func() {
			if err := server.Start(); err != nil {
				log.Error("start router error: %v", err)
				os.Exit(1)
			}
		}()

		if port := config.Conf().Router.PprofPort; port > 0 {
			debugutil.StartUIPprofListener(int(port))
		}
	}

	var psPath []string
	for k := range paths {
		psPath = append(psPath, k)
	}

	mserver.Start(ctx, psPath)
	mserver.AddLabel("models", strings.Join(models, ","))

	sigsHook.WaitSignals()
	sigsHook.AsyncInvokeHooks()
	sigsHook.WaitUntilTimeout(30 * time.Second)
}

func getDefaultConfigFile() (defaultConfigFile string) {
	if currentExePath, err := getCurrentPath(); err == nil {
		path := filepath.Join(currentExePath, "config", "config.toml")
		if ok, err := pathExists(path); ok {
			return path
		} else if err != nil {
			log.Error("check path: %s err: %s", path, err.Error())
		}
	}

	if sourceCodeFileName, err := getCurrentSourceCodePath(); err == nil {
		lastIndex := strings.LastIndex(sourceCodeFileName, string(os.PathSeparator))
		path := filepath.Join(sourceCodeFileName[:lastIndex+1], "config", "config.toml")
		if ok, err := pathExists(path); ok {
			return path
		} else if err != nil {
			log.Error("check path: %s err: %s", path, err.Error())
		}
	}
	return
}

func getCurrentPath() (string, error) {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		return "", errors.New("cannot get current path")
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(path)
	return dir, nil
}

func getCurrentSourceCodePath() (string, error) {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		return "", errors.New("cannot get current source code path")
	}
	return file, nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
