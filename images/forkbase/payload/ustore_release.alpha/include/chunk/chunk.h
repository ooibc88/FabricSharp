// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_H_
#define USTORE_CHUNK_CHUNK_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "hash/hash.h"
#include "types/type.h"
#include "utils/logging.h"
#include "utils/noncopyable.h"

namespace ustore {

class Chunk : private Moveable {
 public:
  /*
   * Chunk format:
   * | uint32_t  | ChunkType | Bytes |
   * | num_bytes | type      | data  |
   */
  static constexpr size_t kNumBytesOffset = 0;
  static constexpr size_t kChunkTypeOffset = kNumBytesOffset + sizeof(uint32_t);
  static constexpr size_t kMetaLength = kChunkTypeOffset + sizeof(ChunkType);

  // create an empty chunk
  Chunk() : Chunk(nullptr) {}
  // allocate a new chunk with usable capacity (excluding meta data)
  Chunk(ChunkType type, uint32_t capacity);
  // create chunk but not own the data
  explicit Chunk(const byte_t* head) noexcept : head_(head) {}
  // create chunk and let it own the data
  explicit Chunk(std::unique_ptr<byte_t[]> head) noexcept;
  // create chunk with existing hash
  // used by lst store
  Chunk(const byte_t* head, const byte_t* hash) noexcept
    : head_(head), hash_(hash) {}
  // movable
  Chunk(Chunk&&) = default;
  Chunk& operator=(Chunk&&) = default;

  ~Chunk() = default;

  inline bool empty() const noexcept { return head_ == nullptr; }
  // total number of bytes
  inline uint32_t numBytes() const noexcept {
    return *reinterpret_cast<const uint32_t*>(head_ + kNumBytesOffset);
  }
  // type of the chunk
  inline ChunkType type() const noexcept {
    return *reinterpret_cast<const ChunkType*>(head_ + kChunkTypeOffset);
  }
  // number of bytes used to store actual data
  inline uint32_t capacity() const noexcept { return numBytes() - kMetaLength; }

  // pointer to the chunk
  inline const byte_t* head() const noexcept { return head_; }

  // pointer to actual data
  inline const byte_t* data() const noexcept { return head_ + kMetaLength; }

  // pointer to mutable data
  inline byte_t* m_data() const noexcept {
      CHECK(own_ != nullptr);
      return &own_[kMetaLength];
  };

  // chunk hash
  inline const Hash& hash() const {
    return hash_.empty() ? forceHash() : hash_;
  }
  // force to re-compute chunk hash
  inline const Hash& forceHash() const {
    hash_ = Hash::ComputeFrom(head_, numBytes());
    return hash_;
  }

 private:
  // own the chunk if created by itself
  std::unique_ptr<byte_t[]> own_;
  // read-only chunk if passed from chunk storage
  const byte_t* head_ = nullptr;
  mutable Hash hash_;
};

}  // namespace ustore
#endif  // USTORE_CHUNK_CHUNK_H_
