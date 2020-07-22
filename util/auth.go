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

package util

import (
	"encoding/base64"
	"strings"

	"github.com/vearch/vearch/util/log"
)

const (
	HeaderAuthBasic = "Basic "
)

func AuthDecrypt(headerData string) (userName, password string, err error) {
	var dataByte []byte
	basicToken := strings.TrimPrefix(headerData, HeaderAuthBasic)
	dataByte, err = base64.URLEncoding.DecodeString(basicToken)
	if err != nil {
		log.Error("can not decode auth original data. err:%v", err)
		return "", "", err
	}
	dataStr := string(dataByte)
	dataSegments := strings.Split(dataStr, ":")
	if dataSegments == nil || len(dataSegments) != 2 {
		log.Error("split auth data string error")
		return "", "", err
	}

	return dataSegments[0], dataSegments[1], nil
}

func AuthEncrypt(userName, password string) string {
	dataStr := strings.Join([]string{userName, password}, ":")
	basicToken := base64.URLEncoding.EncodeToString([]byte(dataStr))
	headerData := strings.Join([]string{HeaderAuthBasic, basicToken}, "")
	return headerData
}
