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

package response

import (
	"time"
)

type CostTime struct {
	ParamStartTime     time.Time `json:"param_start_time"`
	ParamEndTime       time.Time `json:"param_end_time"`
	GammaStartTime     time.Time `json:"gamma_start_time"`
	GammaEndTime       time.Time `json:"gamma_end_time"`
	DocSStartTime      time.Time `json:"merge_start_time"`
	DocSEndTime        time.Time `json:"merge_end_time"`
	ClientPsPStartTime time.Time `json:"clientpsp_start_time"`
	ClientPsPEndTime   time.Time `json:"clientpsp_end_time"`
	PsHandlerStartTime time.Time `json:"pshandler_start_time"`
	PsHandlerEndTime   time.Time `json:"pshandler_end_time"`
	PsSWStartTime      time.Time `json:"pssw_start_time"`
	PsSWEndTime        time.Time `json:"pssw_end_time"`
}
