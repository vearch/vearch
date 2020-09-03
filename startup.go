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
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/util/vearchlog"

	"github.com/vearch/vearch/util/metrics/mserver"

	"time"

	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/master"
	"github.com/vearch/vearch/ps"
	router "github.com/vearch/vearch/router"
	"github.com/vearch/vearch/util/log"
	tigos "github.com/vearch/vearch/util/runtime/os"
	"github.com/vearch/vearch/util/signals"
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
	flag.StringVar(&masterName, "master", "", "vearch config for master name , is on local start two master must use it")
}

const (
	psTag     = "ps"
	masterTag = "master"
	routerTag = "router"
	allTag    = "all"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Info("start server by version:[%s] commitID:[%s]", BuildVersion, CommitID)
	config.SetConfigVersion(BuildVersion, BuildTime, CommitID)

	flag.Parse()

	if strings.Compare(confPath, "") == 0 {
		log.Error("Can not get the config file ,then exit the program!")
		os.Exit(1)
	}
	log.Info("The Config File Is: %v", confPath)

	config.InitConfig(confPath)

	args := flag.Args()

	if len(args) == 0 {
		args = []string{allTag}
	}

	tags := map[string]bool{allTag: false, psTag: false, routerTag: false, masterTag: false}

	for _, a := range args {
		if _, ok := tags[a]; !ok {
			panic(fmt.Sprintf("not found tags: %s it only support [ps,router, master or all]", a))
		} else {
			tags[a] = true
		}
	}

	logName := strings.ToUpper(strings.Join(args, "-"))
	vearchlog.SetConfig(config.Conf().GetLogFileNum(), 1024*1024*config.Conf().GetLogFileSize())
	log.Regist(vearchlog.NewVearchLog(config.Conf().GetLogDir(), logName, config.Conf().GetLevel(), false))

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

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				log.Debug(fmt.Sprint("mem.Alloc:", mem.Alloc, " mem.TotalAlloc:", mem.TotalAlloc, " mem.HeapAlloc:", mem.HeapAlloc, " mem.HeapSys:", mem.HeapSys, " routing :", runtime.NumGoroutine()))
				time.Sleep(10 * time.Second)
			}
		}()
	}

	sigsHook := signals.NewSignalHook()

	var paths = make(map[string]bool)
	paths[config.Conf().GetDataDir()] = true
	paths[config.Conf().GetLogDir()] = true
	var models []string
	//start master
	if tags[masterTag] || tags[allTag] {

		if err := config.Conf().CurrentByMasterNameDomainIp(masterName); err != nil {
			panic(err)
		}

		if err := config.Conf().Validate(config.Master); err != nil {
			panic(err)
		}

		self := config.Conf().Masters.Self()
		mserver.SetIp(self.Address, true)
		models = append(models, "master")

		s, err := master.NewServer(ctx)
		if err != nil {
			panic(fmt.Sprintf("new master error : %s", err.Error()))
		}
		sigsHook.AddSignalHook(func() {
			s.Stop()
		})
		go func() {
			if err := s.Start(); err != nil {
				log.Error(fmt.Sprintf("start master error :%v", err))
				os.Exit(-1)
			}
		}()

		if port := config.Conf().Masters.Self().PprofPort; port > 0 {
			go func() {
				if err := http.ListenAndServe("0.0.0.0:"+cast.ToString(port), nil); err != nil {
					log.Error(err.Error())
				}
			}()
		}

	}

	//start ps
	if tags[psTag] || tags[allTag] {
		if err := config.Conf().Validate(config.PS); err != nil {
			panic(err)
		}

		server := ps.NewServer(ctx)

		models = append(models, "ps")
		sigsHook.AddSignalHook(func() {
			vearchlog.CloseIfNotNil(server)
		})
		go func() {
			if err := server.Start(); err != nil {
				log.Error(fmt.Sprintf("start ps error :%v", err))
				os.Exit(-1)
			}
		}()

		if port := config.Conf().PS.PprofPort; port > 0 {
			go func() {
				if err := http.ListenAndServe("0.0.0.0:"+cast.ToString(port), nil); err != nil {
					log.Error(err.Error())
				}
			}()
		}
	}

	//start router
	if tags[routerTag] || tags[allTag] {
		if err := config.Conf().Validate(config.Router); err != nil {
			panic(err)
		}
		server, err := router.NewServer(ctx)
		if err != nil {
			panic(fmt.Sprintf("new router error :%v", err))
		}
		models = append(models, "router")
		sigsHook.AddSignalHook(func() {
			cancel()
			server.Shutdown()
		})
		go func() {
			if err := server.Start(); err != nil {
				panic(fmt.Sprintf("start router error :%v", err))
			}
		}()

		if port := config.Conf().Router.PprofPort; port > 0 {
			go func() {
				if err := http.ListenAndServe("0.0.0.0:"+cast.ToString(port), nil); err != nil {
					log.Error(err.Error())
				}
			}()
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
	if currentExePath, err := tigos.GetCurrentPath(); err == nil {
		path := currentExePath + "config/config.toml"
		if ok, err := pathExists(path); ok {
			return path
		} else if err != nil {
			log.Error("check path:%s err : %s", path, err.Error())
		}
	}

	if sourceCodeFileName, err := tigos.GetCurrentSourceCodePath(); nil == err {
		lastIndex := strings.LastIndex(sourceCodeFileName, "/")
		path := sourceCodeFileName[0:lastIndex+1] + "config/config.toml"
		if ok, err := pathExists(path); ok {
			return path
		} else if err != nil {
			log.Error("check path:%s err : %s", path, err.Error())
		}
	}
	return
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
