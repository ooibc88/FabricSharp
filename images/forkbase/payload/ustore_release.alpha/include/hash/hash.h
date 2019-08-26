// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HASH_HASH_H_
#define USTORE_HASH_HASH_H_

#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include "hash/murmurhash.h"
#include "spec/slice.h"
#include "types/type.h"
#include "utils/logging.h"

namespace ustore {

class Hash {
 public:
  static constexpr size_t kByteLength = 20;
  static constexpr size_t kBase32Length = 32;
  // the hash used to represent null value
  static const Hash kNull;

  // decode hash from base32 format
  // if do so, must allocate own value
  static Hash FromBase32(const std::string& base32);
  // compute hash from data
  // if do so, must allocate own value
  static Hash ComputeFrom(const byte_t* data, size_t len);
  static Hash ComputeFrom(const std::string& data);
  // reuse data stored in std::String
  static inline Hash Convert(const std::string& request) {
    return Hash(reinterpret_cast<const byte_t*>(request.data()));
  }

  Hash() = default;
  // movable
  Hash(Hash&&) = default;
  // use existing hash
  Hash(const Hash& hash) noexcept : value_(hash.value_) {}
  // use existing byte array
  explicit Hash(const byte_t* hash) noexcept : value_(hash) {}
  // use existing slice
  explicit Hash(const Slice& slice) noexcept : value_(slice.data()) {}
  // use existing string
  explicit Hash(const std::string& str) noexcept
    : value_(reinterpret_cast<const byte_t*>(str.c_str())) {}
  ~Hash() = default;

  // copy and move assignment
  Hash& operator=(Hash hash) noexcept {
    own_.swap(hash.own_);
    std::swap(value_, hash.value_);
    return *this;
  }

  friend inline bool operator<(const Hash& lhs, const Hash& rhs) noexcept {
    if (!lhs.value_ || !rhs.value_) return lhs.value_ < rhs.value_;
    return std::memcmp(lhs.value_, rhs.value_, Hash::kByteLength) < 0;
  }

  friend inline bool operator>(const Hash& lhs, const Hash& rhs) noexcept {
    if (!lhs.value_ || !rhs.value_) return lhs.value_ > rhs.value_;
    return std::memcmp(lhs.value_, rhs.value_, Hash::kByteLength) > 0;
  }

  friend inline bool operator==(const Hash& lhs, const Hash& rhs) noexcept {
    if (!lhs.value_ || !rhs.value_) return lhs.value_ == rhs.value_;
    return std::memcmp(lhs.value_, rhs.value_, Hash::kByteLength) == 0;
  }

  friend inline bool operator<=(const Hash& lhs, const Hash& rhs) noexcept {
    return !operator>(lhs, rhs);
  }

  friend inline bool operator>=(const Hash& lhs, const Hash& rhs) noexcept {
    return !operator<(lhs, rhs);
  }

  friend inline bool operator!=(const Hash& lhs, const Hash& rhs) noexcept {
    return !operator==(lhs, rhs);
  }

  // check if the hash is empty
  inline bool empty() const { return value_ == nullptr; }
  // check if hash owns the data
  inline bool own() const { return own_.get() != nullptr; }
  // expose byte array to others
  inline const byte_t* value() const { return value_; }
  // get a copy that contains own bytes
  Hash Clone() const;
  // encode to base32 format
  std::string ToBase32() const;
  // get a string version copy
  inline std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(value_),
                       Hash::kByteLength);
  }

  friend inline std::ostream& operator<<(std::ostream& os, const Hash& obj) {
    os << (obj == kNull ? "<null>" : obj.ToBase32());
    return os;
  }

 private:
  static const byte_t kEmptyBytes[kByteLength];

  // allocate space if previously not have
  void Alloc();

  // hash own the value if is calculated by itself
  std::unique_ptr<byte_t[]> own_;
  // otherwise the pointer should be read-only
  // big-endian
  const byte_t* value_ = nullptr;
};

}  // namespace ustore

namespace std {
template<>
struct hash<::ustore::Hash> {
  inline size_t operator()(const ::ustore::Hash& obj) const {
    const auto& hval = obj.value();
    size_t ret;
    std::copy(hval, hval + sizeof(ret),
              reinterpret_cast<::ustore::byte_t*>(&ret));
    return ret;
  }
};
}  // namespace std

#endif  // USTORE_HASH_HASH_H_
