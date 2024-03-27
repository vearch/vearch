#include "status.h"

#include <cassert>
#include <cstring>

namespace vearch {

std::unique_ptr<const char[]> Status::CopyState(const char *s) {
  const size_t cch = std::strlen(s) + 1;  // +1 for the null terminator
  char *rv = new char[cch];
  std::strncpy(rv, s, cch);
  return std::unique_ptr<const char[]>(rv);
}

static const char *msgs[static_cast<int>(Status::kMaxSubCode)] = {
    "",                                  // kNone
    "Index not trained",                 // kIndexNotTrained
    "Parameters error",                  // kParamError
    "Resource temporarily unavailable",  // kResourceExhausted
    "No space left on device",           // kNoSpace
    "No such file or directory",         // kPathNotFound
};

Status::Status(status::Code _code, SubCode _subcode, const std::string &msg,
               const std::string &msg2, Severity sev)
    : code_(_code), subcode_(_subcode), sev_(sev) {
  assert(subcode_ != kMaxSubCode);
  const size_t len1 = msg.size();
  const size_t len2 = msg2.size();
  const size_t size = len1 + (len2 ? (2 + len2) : 0);
  char *const result = new char[size + 1];  // +1 for null terminator
  memcpy(result, msg.data(), len1);
  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    memcpy(result + len1 + 2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_.reset(result);
}

std::string Status::ToString() const {
  const char *type = nullptr;
  switch (code_) {
    case status::kOk:
      return "OK";
    case status::kNotFound:
      type = "NotFound: ";
      break;
    case status::kIndexError:
      type = "IndexError: ";
      break;
    case status::kNotSupported:
      type = "Not implemented: ";
      break;
    case status::kInvalidArgument:
      type = "Invalid argument: ";
      break;
    case status::kIOError:
      type = "IO error: ";
      break;
    case status::kBusy:
      type = "Resource busy: ";
      break;
    case status::kTimedOut:
      type = "Operation timed out: ";
      break;
    case status::kMaxCode:
      assert(false);
      break;
  }
  char tmp[30];
  if (type == nullptr) {
    // This should not happen since `code_` should be a valid non-`kMaxCode`
    // member of the `Code` enum. The above switch-statement should have had a
    // case assigning `type` to a corresponding string.
    assert(false);
    snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
    type = tmp;
  }
  std::string result(type);
  if (subcode_ != kNone) {
    uint32_t index = static_cast<int32_t>(subcode_);
    assert(sizeof(msgs) / sizeof(msgs[0]) > index);
    result.append(msgs[index]);
  }

  if (state_ != nullptr) {
    if (subcode_ != kNone) {
      result.append(": ");
    }
    result.append(state_.get());
  }
  return result;
}
}  // namespace vearch
