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
package tracer

import (
	"fmt"
	"io"
	"time"

	jaeger "github.com/uber/jaeger-client-go"
	config "github.com/uber/jaeger-client-go/config"
	vconfig "github.com/vearch/vearch/v3/internal/config"
)

// InitJaeger returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func InitJaeger(service string, c *vconfig.TracerCfg) io.Closer {
	cfg := &config.Configuration{
		ServiceName: service,
		Sampler: &config.SamplerConfig{
			Type:  c.SampleType,
			Param: c.SampleParam,
		},
		Reporter: &config.ReporterConfig{
			LocalAgentHostPort:         c.Host,
			LogSpans:                   false,
			DisableAttemptReconnecting: false,
			AttemptReconnectInterval:   1 * time.Minute,
		},
	}
	closer, err := cfg.InitGlobalTracer(service, config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return closer
}
