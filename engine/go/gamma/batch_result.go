/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	"../../idl/fbs-gen/go/gamma_api"
)

type BatchResult struct {
	Codes        []int32
	Msgs         []string
	batch_result *gamma_api.BatchResult
}

func (batch_result *BatchResult) DeSerialize(buffer []byte) {
	batch_result.batch_result = gamma_api.GetRootAsBatchResult(buffer, 0)
	batch_result.Codes = make([]int32, batch_result.batch_result.CodesLength())
	batch_result.Msgs = make([]string, batch_result.batch_result.MsgsLength())
	for i := 0; i < len(batch_result.Codes); i++ {
		batch_result.Msgs[i] = string(batch_result.batch_result.Msgs(i))
		batch_result.Codes[i] = batch_result.batch_result.Codes(i)
	}
}
