# C API User Guide for Gamma Engine

This folder include gamma-engine C-style API.

* example

``` test_code
#include "c_api/gamma_api.h"

int main(int argc, char **argv) {
  tig_gamma::Config config;
  config.SetPath("path");
  config.SetLogDir("log);

  char *config_str = nullptr;
  int len = 0;
  config.Serialize(&config_str, &len);
  void *engine = Init(config_str, len);
  free(config_str);
  config_str = nullptr;
  Close(engine);
  return 0;
}
```
