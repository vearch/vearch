#pragma once

#include <memory>
#include <string>
#include <utility>

#include "idl/fbs-gen/c/status_generated.h"

namespace vearch {

class Status {
 public:
  Status()
      : code_(status::kOk), subcode_(kNone), sev_(kNoError), state_(nullptr) {}
  ~Status() {}

  Status(const Status &s);
  Status &operator=(const Status &s);
  Status(Status &&s) noexcept;
  Status &operator=(Status &&s) noexcept;

  bool operator==(const Status &rhs) const;
  bool operator!=(const Status &rhs) const;

  status::Code code() const { return code_; }

  enum SubCode : unsigned char {
    kNone = 0,
    kIndexNotTrained = 1,
    kParamError = 2,
    kResourceExhausted = 3,
    kNoSpace = 4,
    kPathNotFound = 5,
    kMemoryExceeded = 6,
    kMaxSubCode
  };

  SubCode subcode() const { return subcode_; }

  enum Severity : unsigned char {
    kNoError = 0,
    kSoftError = 1,
    kHardError = 2,
    kFatalError = 3,
    kUnrecoverableError = 4,
    kMaxSeverity
  };

  Status(const Status &s, Severity sev);
  Severity severity() const { return sev_; }

  // Returns a C style string indicating the message of the Status
  const char *getState() const { return state_.get(); }

  // Return a success status.
  static Status OK() { return Status(); }

  static Status NotSupported(const std::string &msg,
                             const std::string &msg2 = std::string()) {
    return Status(status::kNotSupported, msg, msg2);
  }
  static Status NotSupported(SubCode msg = kNone) {
    return Status(status::kNotSupported, msg);
  }

  static Status InvalidArgument(const std::string &msg,
                                const std::string &msg2 = std::string()) {
    return Status(status::kInvalidArgument, msg, msg2);
  }
  static Status InvalidArgument(SubCode msg = kNone) {
    return Status(status::kInvalidArgument, msg);
  }

  static Status ParamError(const std::string &msg,
                           const std::string &msg2 = std::string()) {
    return Status(status::kInvalidArgument, kParamError, msg, msg2);
  }
  static Status ParamError() {
    return Status(status::kInvalidArgument, kParamError);
  }

  static Status ResourceExhausted(const std::string &msg,
                                  const std::string &msg2 = std::string()) {
    return Status(status::kBusy, kResourceExhausted, msg, msg2);
  }
  static Status ResourceExhausted() {
    return Status(status::kBusy, kResourceExhausted);
  }

  static Status IndexError(SubCode msg = kNone) {
    return Status(status::kIndexError, msg);
  }
  static Status IndexError(const std::string &msg,
                           const std::string &msg2 = std::string()) {
    return Status(status::kIndexError, msg, msg2);
  }

  static Status IndexNotTrained(const std::string &msg,
                                const std::string &msg2 = std::string()) {
    return Status(status::kIndexError, kIndexNotTrained, msg, msg2);
  }
  static Status IndexNotTrained() {
    return Status(status::kIndexError, kIndexNotTrained);
  }

  static Status TimedOut(SubCode msg = kNone) {
    return Status(status::kTimedOut, msg);
  }
  static Status TimedOut(const std::string &msg,
                         const std::string &msg2 = std::string()) {
    return Status(status::kTimedOut, msg, msg2);
  }

  static Status IOError(SubCode msg = kNone) {
    return Status(status::kIOError, msg);
  }
  static Status IOError(const std::string &msg,
                        const std::string &msg2 = std::string()) {
    return Status(status::kIOError, msg, msg2);
  }

  static Status PathNotFound() {
    return Status(status::kIOError, kPathNotFound);
  }
  static Status PathNotFound(const std::string &msg,
                             const std::string &msg2 = std::string()) {
    return Status(status::kIOError, kPathNotFound, msg, msg2);
  }

  static Status MemoryExceeded(SubCode msg = kNone) {
    return Status(status::kMemoryExceeded, msg);
  }
  static Status MemoryExceeded(const std::string &msg,
                             const std::string &msg2 = std::string()) {
    return Status(status::kMemoryExceeded, msg, msg2);
  }

  static Status RequestCanceled(SubCode msg = kNone) {
    return Status(status::kCanceled, msg);
  }
  static Status RequestCanceled(const std::string &msg,
                             const std::string &msg2 = std::string()) {
    return Status(status::kCanceled, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return code() == status::kOk; }

  std::string ToString() const;

 private:
  // A nullptr state_ (which is always the case for OK) means the message
  // is empty.
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4..]  == message
  status::Code code_;
  SubCode subcode_;
  Severity sev_;
  std::unique_ptr<const char[]> state_;

  explicit Status(status::Code _code, SubCode _subcode = kNone)
      : code_(_code), subcode_(_subcode), sev_(kNoError) {}

  explicit Status(status::Code _code, SubCode _subcode, bool retryable,
                  bool data_loss, unsigned char scope)
      : code_(_code), subcode_(_subcode), sev_(kNoError) {}

  Status(status::Code _code, SubCode _subcode, const std::string &msg,
         const std::string &msg2, Severity sev = kNoError);
  Status(status::Code _code, const std::string &msg, const std::string &msg2)
      : Status(_code, kNone, msg, msg2) {}

  std::unique_ptr<const char[]> CopyState(const char *s);
};

inline bool Status::operator==(const Status &rhs) const {
  return (code_ == rhs.code_);
}

inline Status::Status(const Status &s)
    : code_(s.code_), subcode_(s.subcode_), sev_(s.sev_) {
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_.get());
}
inline Status::Status(const Status &s, Severity sev)
    : code_(s.code_), subcode_(s.subcode_), sev_(sev) {
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_.get());
}
inline Status &Status::operator=(const Status &s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (this != &s) {
    code_ = s.code_;
    subcode_ = s.subcode_;
    sev_ = s.sev_;
    state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_.get());
  }
  return *this;
}

inline Status::Status(Status &&s) noexcept : Status() { *this = std::move(s); }

inline Status &Status::operator=(Status &&s) noexcept {
  if (this != &s) {
    code_ = std::move(s.code_);
    s.code_ = status::kOk;
    subcode_ = std::move(s.subcode_);
    s.subcode_ = kNone;
    sev_ = std::move(s.sev_);
    s.sev_ = kNoError;
    state_ = std::move(s.state_);
  }
  return *this;
}

inline bool Status::operator!=(const Status &rhs) const {
  return !(*this == rhs);
}
}  // namespace vearch
