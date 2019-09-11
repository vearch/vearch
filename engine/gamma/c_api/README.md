# C API User Guide for Gamma Engine

This folder include gamma-engine C-style API.

* example

``` test_code
#include "c_api.h"

int main(int argc, char **argv) {
  ByteArray *path = MakeByteArray("path", 4);
  Config *config = MakeConfig(StringToByteArray(path), 1000);
  void *engine = Init(config);
  Close(engine);
  return 0;
}
```
