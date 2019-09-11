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

package bufalloc

import (
	"testing"

	"github.com/vearch/vearch/util/bytes"
)

func TestGetPoolNum(t *testing.T) {
	for i, n := range buffPool.baseline {
		num := buffPool.getPoolNum(n)
		if num != i {
			t.Errorf("Got %v expected %v", num, i)
		}
		num = buffPool.getPoolNum(n - 1)
		if num != i {
			t.Errorf("Got %v expected %v", num, i)
		}
	}
	num := buffPool.getPoolNum(2 * bytes.MB)
	if num != baseSize {
		t.Errorf("Got %v expected %v", num, baseSize)
	}
}

func TestGetBuffer(t *testing.T) {
	for _, n := range buffPool.baseline {
		buf := buffPool.getBuffer(n)
		if buf.Len() != 0 || buf.Cap() != n {
			t.Errorf("Got %v expected %v", buf.Cap(), n)
		}
		buffPool.putBuffer(buf)
	}
	buf := buffPool.getBuffer(2 * bytes.MB)
	if buf.Len() != 0 || buf.Cap() != 2*bytes.MB {
		t.Errorf("Got %v expected %v", buf.Cap(), 2*bytes.MB)
	}
	buffPool.putBuffer(buf)
}
