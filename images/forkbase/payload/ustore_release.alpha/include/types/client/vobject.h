// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VOBJECT_H_
#define USTORE_TYPES_CLIENT_VOBJECT_H_

#include "spec/value.h"

namespace ustore {

class VObject {
 public:
  VObject() = default;
  virtual ~VObject() = default;

  bool empty() const { return buffer_.type == UType::kUnknown; }
  const Value& value() const { return buffer_; }

  void SetContext(Slice ctx) { buffer_.ctx = ctx; }
  void Clear() { buffer_ = {}; }

 protected:
  mutable Value buffer_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VOBJECT_H_
