// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_ORDEREDKEY_H_
#define USTORE_NODE_ORDEREDKEY_H_

#include <cstddef>

#include "spec/slice.h"
#include "types/type.h"
#include "utils/logging.h"

namespace ustore {
class OrderedKey {
  /* OrderedKey can either be a hash value (byte array) or an uint64_t integer

   Encoding Scheme by OrderedKey (variable size)
   |-----hash value/uint64 |
   0  -- variable size
 */
 public:
  inline static OrderedKey FromSlice(const Slice& key) {
    return OrderedKey(false, key.data(), key.len());
  }
  // Set an integer value for key
  // own set to false
  OrderedKey() = default;
  explicit OrderedKey(uint64_t value) noexcept;
  // Set the hash data for key
  OrderedKey(bool by_value, const byte_t* data, size_t num_bytes) noexcept;
  OrderedKey(const OrderedKey&) = default;
  ~OrderedKey() = default;

  OrderedKey& operator=(const OrderedKey&) = default;

  inline const byte_t* data() const { return slice_.data(); }
  inline size_t numBytes() const {
    return by_value_ ? sizeof(uint64_t) : slice_.len();
  }
  // encode OrderedKey into buffer
  // given buffer capacity > numBytes
  size_t Encode(byte_t* buffer) const;
  inline bool byValue() const { return by_value_; }

  bool operator>(const OrderedKey& otherKey) const;
  bool operator<(const OrderedKey& otherKey) const;
  bool operator==(const OrderedKey& otherKey) const;
  inline bool operator<=(const OrderedKey& otherKey) const {
    return *this < otherKey || *this == otherKey;
  }
  inline bool operator>=(const OrderedKey& otherKey) const {
    return *this > otherKey || *this == otherKey;
  }

  inline Slice ToSlice() const {
    CHECK(!by_value_);
    return slice_;
  }

  inline bool empty() const {
    return slice_.empty();
  }

 private:
  // Parse the data as a number to compare
  // Otherwise as hash value
  bool by_value_;
  uint64_t value_;
  Slice slice_;
};

}  // namespace ustore

#endif  // USTORE_NODE_ORDEREDKEY_H_
