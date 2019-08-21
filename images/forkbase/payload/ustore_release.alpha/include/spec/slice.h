// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_SLICE_H_
#define USTORE_SPEC_SLICE_H_

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include "hash/murmurhash.h"
#include "types/type.h"

namespace ustore {

/**
 * Slice is a unified type for C and C++ style strings.
 * It only points to the head of original string, does not copy the content.
 */
class Slice {
 public:
  Slice() = default;
  Slice(const Slice&) = default;
  Slice(const byte_t* slice, size_t len) : data_(slice), len_(len) {}
  // share data from c string
  Slice(const char* slice, size_t len)
    : Slice(reinterpret_cast<const byte_t*>(slice), len) {}
  explicit Slice(const char* slice) : Slice(slice, std::strlen(slice)) {}
  // share data from c++ string
  explicit Slice(const std::string& slice)
    : Slice(slice.data(), slice.length()) {}
  // delete constructor that takes in rvalue std::string
  //   to avoid the memory space of parameter is released unawares.
  explicit Slice(std::string&&) = delete;
  ~Slice() = default;

  Slice& operator=(const Slice&) = default;

  inline bool operator<(const Slice& slice) const {
    size_t min_len = std::min(len_, slice.len_);
    int cmp = std::memcmp(data_, slice.data_, min_len);
    return cmp ? cmp < 0 : len_ < slice.len_;
  }
  inline bool operator>(const Slice& slice) const {
    size_t min_len = std::min(len_, slice.len_);
    int cmp = std::memcmp(data_, slice.data_, min_len);
    return cmp ? cmp > 0 : len_ > slice.len_;
  }
  inline bool operator==(const Slice& slice) const {
    if (len_ != slice.len_) return false;
    return std::memcmp(data_, slice.data_, len_) == 0;
  }
  inline bool operator!=(const Slice& slice) const {
    return !operator==(slice);
  }
  inline bool operator==(const std::string& str) const {
    if (len_ != str.size()) return false;
    return std::memcmp(data_, str.c_str(), len_) == 0;
  }
  inline bool operator!=(const std::string& str) const {
    return !operator==(str);
  }
  friend inline bool operator==(const std::string& str, const Slice& slice) {
    return slice == str;
  }
  friend inline bool operator!=(const std::string& str, const Slice& slice) {
    return str != slice;
  }

  inline bool empty() const { return len_ == 0; }
  inline size_t len() const { return len_; }
  inline const byte_t* data() const { return data_; }

  inline std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(data_), len_);
  }

  friend inline std::ostream& operator<<(std::ostream& os, const Slice& obj) {
    // avoid memory copy here
    // os << obj.ToString();
    const char* ptr = reinterpret_cast<const char*>(obj.data_);
    for (size_t i = 0; i < obj.len_; ++i) os << *(ptr++);
    return os;
  }

 private:
  const byte_t* data_ = nullptr;
  size_t len_ = 0;
};

/* Slice variant to ensure always point to valid string when used in containers.
 * Copy a string when stored in containers, not copy when lookup.
 */
class PSlice : public Slice {
 public:
  // Persist a slice
  static PSlice Persist(const Slice& slice) {
    PSlice ps(slice);
    // create own string and point to it
    ps.value_ = slice.ToString();
    ps.Slice::operator=(Slice(ps.value_));
    return ps;
  }

  // Do not persist a slice
  PSlice(const Slice& slice) : Slice(slice) {}  // NOLINT
  // Persist only when already point to own string
  PSlice(const PSlice& pslice) : Slice(pslice), value_(pslice.value_) {
    if (!value_.empty()) this->Slice::operator=(Slice(value_));
  }
  PSlice(PSlice&& pslice) : Slice(pslice), value_(std::move(pslice.value_)) {
    if (!value_.empty()) this->Slice::operator=(Slice(value_));
  }

  inline PSlice& operator=(PSlice pslice) {
    std::swap(value_, pslice.value_);
    this->Slice::operator=(value_.empty() ? pslice : Slice(value_));
    return *this;
  }

 private:
  std::string value_;
};

}  // namespace ustore

namespace std {

template<>
struct hash<::ustore::Slice> {
  inline size_t operator()(const ::ustore::Slice& obj) const {
    return ::ustore::MurmurHash(obj.data(), obj.len());
  }
};

template<>
struct hash<::ustore::PSlice> {
  inline size_t operator()(const ::ustore::PSlice& obj) const {
    return hash<::ustore::Slice>()(obj);
  }
};

}  // namespace std

#endif  // USTORE_SPEC_SLICE_H_
