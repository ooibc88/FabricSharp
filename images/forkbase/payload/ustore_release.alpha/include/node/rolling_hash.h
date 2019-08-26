// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_ROLLING_HASH_H_
#define USTORE_NODE_ROLLING_HASH_H_

#include <cstddef>

#include "hash/buzhash.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class RollingHasher : private Noncopyable {
 public:
  // NOTE: these default values are not used by chunkers,
  //       as they have own default values
  // 4KB -- expect boundary pattern
  static constexpr uint32_t kDefaultChunkPattern = (1 << 12) - 1;
  static constexpr size_t kDefaultChunkWindow = 256;
  // 32KB -- hard upper bound of the size of chunks
  static constexpr size_t kDefaultMaxChunkSize = 1 << 15;

#ifdef TEST_NODEBUILDER
  // a specific rolling hasher for testing purpose
  // with smaller window size and greater probabiltiy of boundary detection
  inline static RollingHasher* TestHasher() {
    // 256B chunk pattern
    // 1KB maximum chunk size
    // 32B rolling hash window size
    return new RollingHasher(uint32_t((1 << 8) - 1), 32, 1 << 9);
  }
#endif  // TEST_NODEBUILDER

  RollingHasher();
  RollingHasher(uint32_t chunk_pattern, size_t window_size, size_t max_size);

  inline void HashByte(byte_t b) {
    ++byte_hashed_;
    buz_.HashByte(b);
    crossed_boundary_ = (byte_hashed_ >= window_size_) &&
                        (crossed_boundary_ ||
                         ((buz_.Sum32() & chunk_pattern_) == chunk_pattern_) ||
                         (byte_hashed_ == max_size_));
  }

  void HashBytes(const byte_t* data, size_t numBytes);
  size_t TryHashBytes(const byte_t* data, size_t numBytes);
  inline void ClearLastBoundary() {
    crossed_boundary_ = false;
    byte_hashed_ = 0;
  }
  inline void ResetBoundary() { crossed_boundary_ = false; }
  inline bool CrossedBoundary() { return crossed_boundary_; }
  inline size_t window_size() { return window_size_; }
  inline size_t byte_hashed() { return byte_hashed_; }

 private:
  uint32_t chunk_pattern_;
  size_t window_size_, max_size_, byte_hashed_ = 0;
  bool crossed_boundary_ = false;
  buzhash::BuzHash buz_;
};
}  // namespace ustore

#endif  // USTORE_NODE_ROLLING_HASH_H_
