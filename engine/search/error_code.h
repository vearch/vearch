/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef ERROR_CODE_H_
#define ERROR_CODE_H_

/*
 * error code description
 * 100~200: general errors, it is mostly caused by input paramaters or data
 * 200~300: io error, file io error or read/write from/to db error
 * 300~400: internal error, in most cases, it is caused by a system call error
 */
#define SUCC 0            // success
#define PARAM_ERR 100     // parameter error
#define FORMAT_ERR 101    // format error
#define IO_ERR 200        // io error
#define INTERNAL_ERR 300  // internal error
#define ALLOC_ERR 301     // bad memory allocation
#define SYSTEM_ERR 302    // system call error
#define LIMIT_ERR 303     // exceed system limit error
#define TIMEOUT_ERR 304   // timeout error

#endif
