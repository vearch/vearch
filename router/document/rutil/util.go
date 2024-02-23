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

package rutil

import (
	"fmt"

	"github.com/bytedance/sonic"
)

func RowDateToUInt8Array(data []byte, dimension int) ([]uint8, error) {
	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []uint8

	if err := sonic.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func RowDateToFloatArray(data []byte, dimension int) ([]float32, error) {
	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []float32

	if err := sonic.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	if result != nil && (len(result)%dimension) != 0 {
		return nil, fmt.Errorf("vector query length err, not equals dimension multiple:[%d]", (len(result) % dimension))
	}

	return result, nil
}
