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
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

func NewQuery() *query {
	query := &query{
		header: make(map[string]string),
	}
	return query
}

type query struct {
	header  map[string]string
	method  string
	url     string
	address string
	urlPath string
	query   string
	reqBody string
	timeout int64
}

func (this *query) SetMethod(method string) *query {
	this.method = method
	return this
}

func (this *query) SetUrl(url string) *query {
	this.url = url
	return this
}

func (this *query) SetAddress(address string) *query {
	this.address = address
	return this
}

func (this *query) SetUrlPath(urlPath string) *query {
	this.urlPath = urlPath
	return this
}

func (this *query) SetReqBody(reqBody string) *query {
	this.reqBody = reqBody
	return this
}

func (this *query) SetQuery(query string) *query {
	this.query = query
	return this
}

func (this *query) SetTimeout(timeout int64) *query {
	this.timeout = timeout
	return this
}

func (this *query) SetHeader(key string, value string) *query {
	this.header[key] = value
	return this
}

func (this *query) SetContentTypeJson() *query {
	this.SetHeader("Content-Type", "application/json")
	return this
}

func (this *query) SetContentTypeForm() *query {
	this.SetHeader("Content-Type", "application/x-www-form-urlencoded")
	return this
}

func (this *query) GetUrl() string {
	if this.url != "" {
		return this.url
	}

	url := this.address + this.urlPath
	if this.query != "" {
		url = url + "?" + this.query
	}
	return url
}

func (this *query) Do() ([]byte, error) {
	url := this.GetUrl()
	request, _ := http.NewRequest(this.method, url, strings.NewReader(this.reqBody))
	//request.Close = true
	// header
	for key, value := range this.header {
		request.Header.Add(key, value)
	}
	// timeout
	if this.timeout > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(this.timeout)*time.Second, func() {
			cancel()
		})
		request = request.WithContext(ctx)
	}
	// do request
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	//defer request.Body.Close()
	respBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// not 200 ok
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Query error, url: %s, method: %s, resp body: %s, req body: %s", url, this.method, respBody, this.reqBody)
	}

	return respBody, nil
}

func (this *query) DoResponse() (*http.Response, error) {
	url := this.GetUrl()
	request, _ := http.NewRequest(this.method, url, strings.NewReader(this.reqBody))
	//request.Close = true
	// header
	for key, value := range this.header {
		request.Header.Add(key, value)
	}
	// timeout
	if this.timeout > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(this.timeout)*time.Second, func() {
			cancel()
		})
		request = request.WithContext(ctx)
	}
	//defer request.Body.Close()
	// do request
	return http.DefaultClient.Do(request)
}
