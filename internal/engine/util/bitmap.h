/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace bitmap {

/* init a bitmap of which the total length is size */
int create(char *&bitmap, int &bytes_count, int size);

/* assume id not exceed the total size of bitmap */
bool test(const char *bitmap, int id);

/* assume id not exceed the total size of bitmap */
void set(char *bitmap, int id);

/* assume id not exceed the total size of bitmap */
void unset(char *bitmap, int id);

}  // namespace bitmap
