// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNKER_H_
#define USTORE_CHUNK_CHUNKER_H_

#include <memory>
#include <vector>

#include "chunk/chunk.h"
#include "chunk/segment.h"
#include "node/rolling_hash.h"

namespace ustore {

struct ChunkInfo {
  Chunk chunk;
  // a Segment that holding a single MetaEntry bytes
  std::unique_ptr<const Segment> meta_seg;
};

class Chunker {
  // An interface to make chunk from multiple segments.
  //   Each type, e.g, Blob, MetaNode shall have one.
 public:
  virtual ~Chunker() = default;
  virtual ChunkInfo Make(const std::vector<const Segment*>& segments) const = 0;
  virtual bool isFixedEntryLen() const = 0;
  // Derived class can customize rolling hashes
  inline void SetRHasherParams(uint32_t chunk_pattern, size_t chunk_window,
                               size_t max_chunk_size) {
    chunk_pattern_ = chunk_pattern;
    chunk_window_ = chunk_window;
    max_chunk_size_ = max_chunk_size;
  }
  virtual inline std::unique_ptr<RollingHasher> GetRHasher() const {
#ifdef TEST_NODEBUILDER
    return std::unique_ptr<RollingHasher>(RollingHasher::TestHasher());
#else
    return std::unique_ptr<RollingHasher>(
        new RollingHasher(chunk_pattern_, chunk_window_, max_chunk_size_));
#endif
  }

 private:
  uint32_t chunk_pattern_ = (1 << 12) - 1;  // 4KB
  size_t chunk_window_ = 256;
  size_t max_chunk_size_ = 1 << 15;  // 32KB
};

}  // namespace ustore
#endif  // USTORE_CHUNK_CHUNKER_H_
