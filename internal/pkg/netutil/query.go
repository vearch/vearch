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
	"io"
	"net/http"
	"strings"
	"time"
)

type UriParams interface {
	ByName(name string) string
	Put(name string, value string)
}

type UriParamsMap struct {
	values map[string]string
}

func (p *UriParamsMap) Put(name, value string) {
	p.values[name] = value
}

func (p *UriParamsMap) ByName(name string) string {
	if len(name) == 0 {
		return ""
	}
	v, ok := p.values[name]
	if !ok {
		return ""
	} else {
		return v
	}
}

func NewMockUriParams(values map[string]string) UriParams {
	return &UriParamsMap{
		values: values,
	}
}

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

func (q *query) SetMethod(method string) *query {
	q.method = method
	return q
}

func (q *query) SetUrl(url string) *query {
	q.url = url
	return q
}

func (q *query) SetAddress(address string) *query {
	q.address = address
	return q
}

func (q *query) SetUrlPath(urlPath string) *query {
	q.urlPath = urlPath
	return q
}

func (q *query) SetReqBody(reqBody string) *query {
	q.reqBody = reqBody
	return q
}

func (q *query) SetQuery(query string) *query {
	q.query = query
	return q
}

func (q *query) SetTimeout(timeout int64) *query {
	q.timeout = timeout
	return q
}

func (q *query) SetHeader(key string, value string) *query {
	q.header[key] = value
	return q
}

func (q *query) SetContentTypeJson() *query {
	q.SetHeader("Content-Type", "application/json")
	return q
}

func (q *query) SetContentTypeForm() *query {
	q.SetHeader("Content-Type", "application/x-www-form-urlencoded")
	return q
}

func (q *query) GetUrl() string {
	if q.url != "" {
		return q.url
	}

	url := q.address + q.urlPath
	if q.query != "" {
		url = url + "?" + q.query
	}
	return url
}

func (q *query) Do() ([]byte, error) {
	url := q.GetUrl()
	request, _ := http.NewRequest(q.method, url, strings.NewReader(q.reqBody))
	//request.Close = true
	// header
	for key, value := range q.header {
		request.Header.Add(key, value)
	}
	// timeout
	if q.timeout > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(q.timeout)*time.Second, func() {
			cancel()
		})
		request = request.WithContext(ctx)
	}
	// do request
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer request.Body.Close()
	respBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// not 200 ok
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query error, url: %s, method: %s, resp body: %s, req body: %s", url, q.method, respBody, q.reqBody)
	}

	return respBody, nil
}

func (q *query) ProxyDo() (respBody []byte, statusCode int, err error) {
	url := q.GetUrl()
	request, _ := http.NewRequest(q.method, url, strings.NewReader(q.reqBody))
	//request.Close = true
	// header
	for key, value := range q.header {
		request.Header.Add(key, value)
	}
	// timeout
	if q.timeout > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(q.timeout)*time.Second, func() {
			cancel()
		})
		request = request.WithContext(ctx)
	}
	// do request
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, -1, err
	}

	defer request.Body.Close()
	respBody, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, -1, err
	}

	return respBody, response.StatusCode, nil
}

func (q *query) DoResponse() (*http.Response, error) {
	url := q.GetUrl()
	request, _ := http.NewRequest(q.method, url, strings.NewReader(q.reqBody))
	//request.Close = true
	// header
	for key, value := range q.header {
		request.Header.Add(key, value)
	}
	// timeout
	if q.timeout > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(q.timeout)*time.Second, func() {
			cancel()
		})
		request = request.WithContext(ctx)
	}
	//defer request.Body.Close()
	// do request
	return http.DefaultClient.Do(request)
}
