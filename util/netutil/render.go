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

package netutil

import (
	"github.com/vearch/vearch/util/cbjson"
	"net/http"

	"github.com/vearch/vearch/util/log"
)

var (
	textContentType = []string{"text/plain; charset=utf-8"}
	jsonContentType = []string{"application/json; charset=utf-8"}
)

// JSONRender serializes the given struct as JSON into the response body.
func JSONRender(w http.ResponseWriter, data interface{}) {
	jsonBytes, err := cbjson.Marshal(data)
	if err != nil {
		ErrorRender(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header()["Content-Type"] = jsonContentType
	w.Write(jsonBytes)
	log.Debug("render response: %v", string(jsonBytes))
}

// ErrorRender replies to the request with the specified error message and 500 code.
func ErrorRender(w http.ResponseWriter, code int, err error) {
	w.WriteHeader(code)
	w.Header()["Content-Type"] = textContentType
	w.Header().Set("X-Content-Type-Options", "nosniff")
	log.Error("response: status=%v, body=%v", code, err.Error())
}
