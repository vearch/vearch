/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "reflector.h"

Reflector &reflector() {
  static Reflector reflector;
  return reflector;
}
