/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <openssl/md5.h>
#include <string.h>

#include <cassert>
#include <functional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <random>

#include "cjson/cJSON.h"

#define CHECK_DELETE(ptr) \
  {                       \
    if (ptr) {            \
      delete ptr;         \
      ptr = nullptr;      \
    }                     \
  }

#define CHECK_DELETE_ARRAY(ptr) \
  {                             \
    if (ptr) {                  \
      delete[] ptr;             \
      ptr = nullptr;            \
    }                           \
  }

namespace utils {

long get_file_size(const char *path);
long get_file_size(const std::string &path);

std::vector<std::string> split(const std::string &p_str,
                               const std::string &p_separator);

std::vector<std::string> split(const std::string &p_str,
                               const char *p_separator);

int count_lines(const char *filename);

void GenRandom(std::mt19937 &rng, unsigned *addr, unsigned size, unsigned N);

double elapsed();

double getmillisecs();

int isFolderExist(const char *path);

bool file_exist(const std::string &path);

int make_dir(const char *path);

int remove_dir(const char *dir);

int move_dir(const char *src, const char *dst, bool backup = false);

inline char file_sepator() { return '/'; }

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

template <class T>
inline T *NewArray(int len, const char *msg) {
  assert(len > 0);
  T *data = new (std::nothrow) T[len];
  if (data == nullptr) {
    throw std::runtime_error("new array error, " + std::string(msg));
  }
  return data;
}

std::string join(const std::vector<std::string> &strs, char separator);

template <class T>
std::string join(const T *a, int n, char separator) {
  std::stringstream ss;
  ss << "[";
  for (int i = 0; i < n; i++) {
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
  double total;
  double available;
  double used_rate;
} MEM_PACK;

MEM_PACK *get_memoccupy();

struct JsonParser {
  cJSON *content_;

  JsonParser();
  ~JsonParser();
  int Parse(const char *str);
  int GetInt(const std::string &name, int &value) const;
  int GetDouble(const std::string &name, double &value) const;
  int GetString(const std::string &name, std::string &value) const;
  int GetBool(const std::string &name, bool &value) const;
  int GetObject(const std::string &name, JsonParser &value) const;
  bool Contains(const std::string &name) const;

  int PutString(const std::string &name, const std::string &value);
  int PutInt(const std::string &name, int value);
  int PutDouble(const std::string &name, double value);
  int PutObject(const std::string &name, JsonParser &&jp);
  int PutObject(const std::string &name, JsonParser &jp);
  int PutObject(const std::string &name, cJSON *item);

  bool IsEmpty() const { return content_->child == NULL; }
  void Reset(cJSON *content);
  void MergeRight(JsonParser &other);
  std::string ToStr(bool format = false) const;

  JsonParser &operator=(const JsonParser &other);
};

struct FileIO {
  std::string path;
  FILE *fp;

  FileIO(std::string &file_path);
  ~FileIO();
  int Open(const char *mode);
  size_t Write(const void *data, size_t size, size_t m);
  size_t Read(void *data, size_t size, size_t m);
  bool IsOpen() { return fp != nullptr; }
  std::string Path() { return path; }
};

template <typename callable, class... arguments>
void AsyncWait(int after, callable &&f, arguments &&... args) {
  std::function<typename std::result_of<callable(arguments...)>::type()> task(
      std::bind(std::forward<callable>(f), std::forward<arguments>(args)...));

  std::thread([after, task]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(after));
    task();
  })
      .detach();
}

/** bare-bones unique_ptr
 * this one deletes with delete [] */
template <class T>
struct ScopeDeleter {
  const T *ptr;
  explicit ScopeDeleter(const T *ptr = nullptr) : ptr(ptr) {}
  void release() { ptr = nullptr; }
  void set(const T *ptr_in) { ptr = ptr_in; }
  void swap(ScopeDeleter<T> &other) { std::swap(ptr, other.ptr); }
  ~ScopeDeleter() { delete[] ptr; }
};

/** same but deletes with the simple delete (least common case) */
template <class T>
struct ScopeDeleter1 {
  const T *ptr;
  explicit ScopeDeleter1(const T *ptr = nullptr) : ptr(ptr) {}
  void release() { ptr = nullptr; }
  void set(const T *ptr_in) { ptr = ptr_in; }
  void swap(ScopeDeleter1<T> &other) { std::swap(ptr, other.ptr); }
  ~ScopeDeleter1() { delete ptr; }
};

int64_t StringToInt64(const std::string &src);

}  // namespace utils
