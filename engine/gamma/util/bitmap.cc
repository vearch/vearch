/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "bitmap.h"
#include <stdlib.h>
#include <string.h>

namespace bitmap {

int create(char *&bitmap, int &bytes_count, int size) {
  bytes_count = (size >> 3) + 1;
  bitmap = (char *)malloc(bytes_count);
  if (!bitmap) {
    return -1;
  }
  memset(bitmap, 0, bytes_count);
  return 0;
}

bool test(const char *bitmap, int id) {
  return (bitmap[id >> 3] & (0x1 << (id & 0x7)));
}

void set(char *bitmap, int id) { bitmap[id >> 3] |= (0x1 << (id & 0x7)); }

void clear(char *bitmap, int id) { bitmap[id >> 3] ^= (0x1 << (id & 0x7)); }

} // namespace bitmap
