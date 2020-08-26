// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_USTRING_H_
#define USTORE_TYPES_USTRING_H_

#include <memory>

#include "types/base.h"
#include "types/ucell.h"

namespace ustore {

class UString : public BaseType {
 public:
  inline bool empty() const override { return node_.get() == nullptr; }
  inline size_t len() const { return node_->dataLength(); }
  inline const byte_t* data() const { return node_->data(); }
  inline Slice slice() const { return Slice(data(), len()); }

  friend inline std::ostream& operator<<(std::ostream& os, const UString& obj) {
    os << obj.slice();
    return os;
  }

 protected:
  UString() = default;
  UString(UString&&) = default;
  UString& operator=(UString&&) = default;
  explicit UString(const UCell& cell) noexcept : node_(cell.node()) {}
  ~UString() = default;

 protected:
  // Responsible to remove during destructing
  std::shared_ptr<const CellNode> node_;
};

}  // namespace ustore
#endif  // USTORE_TYPES_USTRING_H_
