// +build !ump

package config

import (
	//if not need support vector go build --tags=vector
	"github.com/vearch/vearch/util/monitoring"
)

func newMonitor(conf *Config, key string) monitoring.Monitor {
	return monitoring.EmptyMonitor{}.New(key)
}
