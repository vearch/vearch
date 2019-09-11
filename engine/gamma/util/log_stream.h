/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SRC_SEARCHER_UTIL_LOG_STREAM_H_
#define SRC_SEARCHER_UTIL_LOG_STREAM_H_

#include <algorithm>
#include <assert.h>
#include <limits>
#include <new>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/time.h>

using std::string;

namespace framework {

const static char endl = 0x01;
const static int FIX_BUFFER_SIZE = 1 << 23;
const static int EXTEND_BUFFER_SIZE = 64;

class LogStreamBuffer {
public:
  LogStreamBuffer() {
    data_ = nullptr;
    curr_ = nullptr;
    length_ = 0;
    avail_ = 0;
    is_newline_ = true;
    is_out_error_ = false;
  }

  ~LogStreamBuffer() {
    if (data_ != nullptr) {
      delete[] data_;
      data_ = nullptr;
    }
  }

  int Create() {
    int size = FIX_BUFFER_SIZE + EXTEND_BUFFER_SIZE;
    data_ = new (std::nothrow) char[size];
    if (data_ == nullptr)
      return -1;
    memset(data_, 0, size);

    curr_ = data_;
    avail_ = FIX_BUFFER_SIZE;
    snprintf(error_msg_, EXTEND_BUFFER_SIZE, "\nno space! max memory is %d\n",
             FIX_BUFFER_SIZE);
    return 0;
  }

  int GetNow(char *buf, int size) {
    time_t ltime;
    time(&ltime);
    struct tm ptm;
    bzero(&ptm, sizeof(tm));
    if (localtime_r(&ltime, &ptm) == nullptr) {
      return -1;
    }
    struct timeval ltime2;
    gettimeofday(&ltime2, nullptr);
    return snprintf(buf, size, "[%d-%02d-%02d %02d:%02d:%02d.%ld] ",
                    1900 + ptm.tm_year, 1 + ptm.tm_mon, ptm.tm_mday,
                    ptm.tm_hour, ptm.tm_min, ptm.tm_sec,
                    static_cast<long>((ltime2.tv_usec) / 1000));
  }

  void Append(const char *buf, int len) {
    if (len == 0 || data_ == nullptr)
      return;
    if (is_newline_) {
      const static int MAX_TIME_SIZE = 64;
      char now_time[MAX_TIME_SIZE + 1];
      int time_len = GetNow(now_time, MAX_TIME_SIZE);
      if (time_len > 0) {
        if (avail_ > time_len) {
          memcpy(curr_, now_time, time_len);
          curr_ += time_len;
          length_ += time_len;
          avail_ -= time_len;
        } else {
          avail_ = 0;
        }
      }
      is_newline_ = false;
    }

    if (avail_ > len) {
      if (len == 1 && buf[0] == endl) {
        is_newline_ = true;
        *curr_ = '\n';
        curr_++;
      } else {
        memcpy(curr_, buf, len);
        curr_ += len;
      }
      length_ += len;
      avail_ -= len;
    } else {
      avail_ = 0;
      if (!is_out_error_) {
        is_out_error_ = true;
        memcpy(curr_, error_msg_, strlen(error_msg_));
      }
    }
  }

  const char *Data() const { return data_; }
  int Length() const { return length_; }
  int Avail() const { return avail_; }

private:
  char *data_;
  char *curr_;
  char error_msg_[EXTEND_BUFFER_SIZE];
  bool is_newline_;
  bool is_out_error_;
  int length_;
  int avail_;
};

const static char LOG_LEVEL_STR[][16] = {"debug", "info", "warn", "error"};
const static char LOG_LEVEL_OUTPUT[][16] = {"DEBUG", "INFO", "WARN", "ERROR"};

class LogStream {
public:
  typedef LogStream self;

  enum LOG_LEVEL {
    LOG_DEBUG = 0,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_NONE,
  };

public:
  LogStream() : level_(LOG_NONE), output_(false) {}

  ~LogStream() {}

  self &operator<<(bool v) {
    if (output_)
      buffer_.Append(v ? "1" : "0", 1);
    return *this;
  }

  self &operator<<(short);
  self &operator<<(unsigned short);
  self &operator<<(int);
  self &operator<<(unsigned int);
  self &operator<<(long);
  self &operator<<(unsigned long);
  self &operator<<(long long);
  self &operator<<(unsigned long long);

  self &operator<<(float v) {
    if (output_)
      *this << static_cast<double>(v);
    return *this;
  }

  self &operator<<(double);

  self &operator<<(char v) {
    if (output_)
      buffer_.Append(&v, 1);
    return *this;
  }

  self &operator<<(unsigned char v) {
    if (output_)
      *this << static_cast<char>(v);
    return *this;
  }

  self &operator<<(const char *v) {
    if (output_ && v != nullptr)
      buffer_.Append(v, strlen(v));
    return *this;
  }

  self &operator<<(const string &v) {
    if (output_)
      buffer_.Append(v.c_str(), v.size());
    return *this;
  }

  void Append(const char *str, int len) {
    if (output_)
      buffer_.Append(str, len);
  }

  self &operator()(int level, const char *file, int line) {
    if (level_ != LOG_NONE && level >= level_) {
      output_ = true;
      const char *p = strrchr(file, '/');
      if (p != nullptr)
        p++;
      else
        p = file;
      *this << "[" << p << ":" << line << "] [" << LOG_LEVEL_OUTPUT[level]
            << "] ";
    } else {
      output_ = false;
    }
    return *this;
  }

  const char *Data() const { return buffer_.Data(); }
  int Length() const { return buffer_.Length(); }
  int Avail() const { return buffer_.Avail(); }

  static LOG_LEVEL GetLevel(const char *level) {
    LOG_LEVEL log_level = LOG_NONE;
    if (strcasecmp(level, LOG_LEVEL_STR[LOG_ERROR]) == 0) {
      log_level = LOG_ERROR;
    } else if (strcasecmp(level, LOG_LEVEL_STR[LOG_DEBUG]) == 0) {
      log_level = LOG_DEBUG;
    } else if (strcasecmp(level, LOG_LEVEL_STR[LOG_WARN]) == 0) {
      log_level = LOG_WARN;
    } else if (strcasecmp(level, LOG_LEVEL_STR[LOG_INFO]) == 0) {
      log_level = LOG_INFO;
    }
    return log_level;
  }

  LOG_LEVEL GetLevel() { return level_; }

  int SetLevel(LOG_LEVEL level) {
    level_ = level;
    if (LOG_NONE == level)
      return 0;
    return buffer_.Create();
  }

  int SetLevel(const char *level) {
    LOG_LEVEL log_level = GetLevel(level);
    return SetLevel(log_level);
  }

private:
  template <typename T> void formatInteger(T);

private:
  static const int MAX_NUMBER_SIZE = 32;

  LogStreamBuffer buffer_;
  LOG_LEVEL level_;
  bool output_;
};

#define ERROR_ONLINE(os) ((os))(LogStream::LOG_ERROR, __FILE__, __LINE__)
#define DEBUG_ONLINE(os) ((os))(LogStream::LOG_DEBUG, __FILE__, __LINE__)
#define INFO_ONLINE(os) ((os))(LogStream::LOG_INFO, __FILE__, __LINE__)
#define WARN_ONLINE(os) ((os))(LogStream::LOG_WARN, __FILE__, __LINE__)

const static char digits[] = "9876543210123456789";
const static char *zero = digits + 9;
const static char digitsHex[] = "0123456789abcdef";

template <typename T> inline bool lt0(T v) { return v < 0; }

inline bool lt0(unsigned char) { return false; }
inline bool lt0(unsigned short) { return false; }
inline bool lt0(unsigned int) { return false; }
inline bool lt0(unsigned long) { return false; }
inline bool lt0(unsigned long long) { return false; }

// Efficient Integer to String Conversions, by Matthew Wilson.
template <typename T> inline size_t convert(char buf[], T value) {
  T i = value;
  char *p = buf;

  do {
    int lsd = i % 10;
    i /= 10;
    *p++ = zero[lsd];
  } while (i != 0);

  if (lt0(value)) {
    *p++ = '-';
  }
  *p = '\0';
  std::reverse(buf, p);

  return p - buf;
}

inline size_t convertHex(char buf[], uintptr_t value) {
  uintptr_t i = value;
  char *p = buf;

  do {
    int lsd = i % 16;
    i /= 16;
    *p++ = digitsHex[lsd];
  } while (i != 0);

  *p = '\0';
  std::reverse(buf, p);
  return p - buf;
}

template <typename T> inline void LogStream::formatInteger(T v) {
  char buf[MAX_NUMBER_SIZE];
  size_t len = convert(buf, v);
  buffer_.Append(buf, len);
}

inline LogStream &LogStream::operator<<(short v) {
  if (output_)
    *this << static_cast<int>(v);
  return *this;
}

inline LogStream &LogStream::operator<<(unsigned short v) {
  if (output_)
    *this << static_cast<unsigned int>(v);
  return *this;
}

inline LogStream &LogStream::operator<<(int v) {
  if (output_)
    formatInteger(v);
  return *this;
}

inline LogStream &LogStream::operator<<(unsigned int v) {
  if (output_)
    formatInteger(v);
  return *this;
}

inline LogStream &LogStream::operator<<(long v) {
  if (output_)
    formatInteger(v);
  return *this;
}

inline LogStream &LogStream::operator<<(unsigned long v) {
  if (output_)
    formatInteger(v);
  return *this;
}

inline LogStream &LogStream::operator<<(long long v) {
  if (output_)
    formatInteger(v);
  return *this;
}

inline LogStream &LogStream::operator<<(unsigned long long v) {
  if (output_)
    formatInteger(v);
  return *this;
}

inline LogStream &LogStream::operator<<(double v) {
  if (output_) {
    char buf[MAX_NUMBER_SIZE];
    int len = snprintf(buf, MAX_NUMBER_SIZE, "%.4f", v);
    buffer_.Append(buf, len);
  }
  return *this;
}

} // namespace framework

#endif // SRC_SEARCHER_UTIL_LOG_STREAM_H_
