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

package bytes

import (
	"bytes"
	"testing"
)

func TestStringToByte(t *testing.T) {
	s := "hello"
	b := StringToByte(s)
	if !bytes.Equal(b, []byte("hello")) {
		t.Errorf("StringToBytes result error: %s->%s", s, string(b))
	}

	b1 := []byte("test")
	s1 := ByteToString(b1)
	if s1 != "test" {
		t.Errorf("BytesToString result error: %s->%s", "test", s1)
	}
}
