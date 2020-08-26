// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VSTRING_H_
#define USTORE_TYPES_CLIENT_VSTRING_H_

#include <memory>
#include "types/client/vobject.h"
#include "types/ustring.h"

namespace ustore {

class VString : public UString, public VObject {
  friend class VMeta;

 public:
  VString() noexcept : VString(Slice()) {}
  VString(VString&&) = default;
  VString& operator=(VString&&) = default;
  // Create new VString
  explicit VString(const Slice& slice) noexcept;
  ~VString() = default;

 protected:
  // Load existing VString
  explicit VString(const UCell& cell) noexcept;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VSTRING_H_
