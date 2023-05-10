/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace tig_gamma {
namespace table {

#ifndef IN
#define IN
#endif

#ifndef OUT
#define OUT
#endif

const static int DOCNUM_PER_SEGMENT = 1 << 20;  // 1048576
const static int MAX_SEGMENT_NUM = 102400;      // max segment num

}  // namespace table
}  // namespace tig_gamma
