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

package document

import (
	"sync"
)

type DocVal struct {
	FieldName string
	Path      []string
}

var pool = sync.Pool{
	New: func() interface{} {
		return &DocVal{}
	},
}

func GetDocVal() *DocVal {
	docV := pool.Get().(*DocVal)
	clean(docV)
	return docV
}

func PutDocVal(docV *DocVal) {
	clean(docV)
	pool.Put(docV)
}

func clean(doc *DocVal) {
	doc.FieldName = ""
	doc.Path = doc.Path[:0]
}
