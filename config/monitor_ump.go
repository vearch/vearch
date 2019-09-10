// +build ump

package config

import (
	. "github.com/vearch/vearch/util/monitoring"
	"github.com/vearch/vearch/util/ump"
	"sync"
)

var once sync.Once

func newMonitor(conf *Config, key string) Monitor {
	once.Do(func() {
		ump.InitUmp(conf.Global.Name)
	})
	return (&ump.UmpMonitor{}).New(key)
}
