/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef UTILS_H_
#define UTILS_H_

#include "gamma_api.h"
#include "cJSON.h"
#include <cassert>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace utils {

long get_file_size(const char *path);

std::vector<std::string> split(const std::string &p_str,
                               const std::string &p_separator);

int count_lines(const char *filename);

double elapsed();

double getmillisecs();

int isFolderExist(const char *path);

int remove_dir(const char *dir);

#ifdef _WIN32

inline char file_sepator() { return '\\'; }
#else

inline char file_sepator() { return '/'; }
#endif

using file_filter_type = std::function<bool(const char *, const char *)>;

std::vector<std::string> for_each_file(const std::string &dir_name,
                                       file_filter_type filter,
                                       bool sub = false);

std::vector<std::string> for_each_folder(const std::string &dir_name,
                                         file_filter_type filter,
                                         bool sub = false);

std::vector<std::string> ls(const std::string &dir_name, bool sub = false);

std::vector<std::string> ls_folder(const std::string &dir_name,
                                   bool sub = false);

ssize_t write_n(int fd, const char *buf, ssize_t nbyte, int retry);

template <class T> inline T *NewArray(int len, const char *msg) {
  assert(len > 0);
  T *data = new (std::nothrow) T[len];
  if (data == nullptr) {
    throw std::runtime_error("new array error, " + std::string(msg));
  }
  return data;
}

std::string join(const std::vector<std::string> &strs, char separator);
template <class T> std::string join(const T *a, int n, char separator) {
  std::stringstream ss;
  ss << "[";
  for (size_t i = 0; i < n; i++) {
    if (i != 0) {
      ss << separator;
    }
    ss << a[i];
  }
  ss << "]";
  return ss.str();
}

typedef struct MEM_PACKED {
  char name[20];
  unsigned long total;
  char name2[20];
} MEM_OCCUPY;

typedef struct MEM_PACK {
  double total, used_rate;
} MEM_PACK;

MEM_PACK *get_memoccupy();

// Based on http://stackoverflow.com/questions/236129/split-a-string-in-c Split
// a string by a delim
inline std::vector<std::string> Split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  if (not s.empty()) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
      elems.push_back(item);
    }
  }
  return elems;
}

struct JsonParser {
  cJSON *content_;

  JsonParser();
  ~JsonParser();
  int Parse(const char *str);
  int GetDouble(const std::string &name, double &value);
  int GetString(const std::string &name, std::string &value);
};

} // namespace utils

#endif /* UTILS_H_ */
