// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_KVDB_STATUS_H_
#define USTORE_KVDB_STATUS_H_

#include <string>

namespace ustore_kvdb {

class Status {
 public:
  Status() : state_(nullptr) {}
  ~Status() { delete[] state_; }
  // copy cotr
  Status(const Status& s);
  Status& operator=(const Status& s);

  static Status OK() { return Status(); }
  static Status NotFound(const std::string& msg,
                         const std::string& msg2 = std::string()) {
    return Status(kNotFound, msg, msg2);
  }
  static Status Corruption(const std::string& msg,
                           const std::string& msg2 = std::string()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status NotSupported(const std::string& msg,
                             const std::string& msg2 = std::string()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const std::string& msg,
                                const std::string& msg2 = std::string()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status IOError(const std::string& msg,
                        const std::string& msg2 = std::string()) {
    return Status(kIOError, msg, msg2);
  }
  bool ok() const { return state_ == nullptr; }
  bool IsNotFound() const { return code() == kNotFound; }
  bool IsCorruption() const { return code() == kCorruption; }
  bool IsNotSupported() const { return code() == kNotSupported; }
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }
  bool IsIOError() const { return code() == kIOError; }
  std::string ToString() const;

 private:
  const char* state_;

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5
  };

  Code code() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const std::string& msg, const std::string& msg2);
  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
}

inline Status& Status::operator=(const Status& s) {
  if (state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
  }
  return *this;
}

}  // namespace ustore_kvdb

#endif  // USTORE_KVDB_STATUS_H_
