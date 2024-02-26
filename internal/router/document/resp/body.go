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

package resp

func NewBodyRootCause(errorType string, reason string, status int) *Body {
	errorRootCause := &ErrorRootCause{}
	errorRootCause.CType = errorType
	errorRootCause.CReason = reason
	errorRootCause.RootCauseArr = make([]RootCause, 1)
	errorRootCause.RootCauseArr[0] = RootCause{CType: errorType, CReason: reason}

	return NewBody(errorRootCause, status)
}

func NewBody(error interface{}, status int) *Body {
	err := &Body{}
	err.Error = error
	err.Status = status

	return err
}

type RootCause struct {
	CType   string `json:"type"`
	CReason string `json:"reason"`
}

type ErrorRootCause struct {
	RootCauseArr []RootCause `json:"root_cause"`
	RootCause
}

type Body struct {
	Error  interface{} `json:"error"`
	Status int         `json:"status"`
}
