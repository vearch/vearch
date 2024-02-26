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

var (
	ErrTypeParseException = "parse_exception"
	ErrTypeAuthException  = "auth_exception"
)

var (
	ErrReasonRequestBodyIsRequired = "request body is required"
	ErrReasonAuthFailed            = "fail to authenticate"
	ErrReasonAuthCodeNotFound      = "fail to authenticate, auth code not found."
	ErrReasonAuthDecryptFailed     = "fail to decrypt auth code, %s"
	ErrReasonUserNotFound          = "user not found"
	ErrReasonIncorrectHttpMethod2  = "Incorrect HTTP method"
	ErrReasonIncorrectHttpMethod   = "Incorrect HTTP method for uri [%s] and method [%s], allowed: [%s]"
)
