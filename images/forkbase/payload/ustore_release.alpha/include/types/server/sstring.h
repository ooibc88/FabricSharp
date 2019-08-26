// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SSTRING_H_
#define USTORE_TYPES_SERVER_SSTRING_H_

#include "types/ustring.h"

namespace ustore {

class SString : public UString {
 public:
  // Load existing VString
  explicit SString(const UCell& cell) noexcept : UString(cell) {}
};  // namespace ustore

}  // namespace ustore

#endif  // USTORE_TYPES_SERVER_SSTRING_H_
