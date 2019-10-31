/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef DEFINITION_LIST_H
#define DEFINITION_LIST_H

#ifdef DefineResultCode

// 0x0000 ~ 0x0FFF  General Status
DefineResultCode(Success, 0x0000)
DefineResultCode(Fail, 0x0001)
DefineResultCode(FailedOpenFile, 0x0002)
DefineResultCode(FailedCreateFile, 0x0003)
DefineResultCode(ParamNotFound, 0x0010)
DefineResultCode(FailedParseValue, 0x0011)

// 0x1000 ~ 0x1FFF  Index Build Status
DefineResultCode(Index_NotFound, 0x1001)
DefineResultCode(Index_WriteError, 0x1002)
DefineResultCode(Index_RepeatedIndex, 0x1003)
DefineResultCode(Index_NoEnoughVectors, 0x1004)
DefineResultCode(Index_NumericIndexError, 0x1010)

// 0x2000 ~ 0x2FFF  Index Serve Status

// 0x3000 ~ 0x3FFF  Record Function Status
DefineResultCode(Record_No_id, 0x3000)
DefineResultCode(Record_DuplicatedId, 0x3001)
DefineResultCode(Record_IdNotFound, 0x3002)

// 0x4000 ~ 0x4FFF  Ini Function Status
DefineResultCode(ReadIni_FailedParseSection, 0x4000)
DefineResultCode(ReadIni_FailedParseParam, 0x4001)
DefineResultCode(ReadIni_DuplicatedSection, 0x4002)
DefineResultCode(ReadIni_DuplicatedParam, 0x4003)

#endif  // DefineResultCode

#endif /* DEFINITION_LIST_H */
