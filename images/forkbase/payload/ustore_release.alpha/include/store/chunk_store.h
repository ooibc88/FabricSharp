// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_CHUNK_STORE_H_
#define USTORE_STORE_CHUNK_STORE_H_

#include <ostream>
#include <string>
#include <unordered_map>
#include "chunk/chunk.h"
#include "store/iterator.h"
#include "types/type.h"

namespace ustore {

struct StoreInfo {
  using MapType = std::unordered_map<ChunkType, size_t>;

  size_t chunks;
  size_t chunkBytes;
  size_t validChunks;
  size_t validChunkBytes;

  size_t maxSegments;
  size_t allocSegments;
  size_t freeSegments;
  size_t usedSegments;

  MapType chunksPerType;
  MapType bytesPerType;

  std::string nodeId;

  friend std::ostream& operator<<(std::ostream& os, const StoreInfo& obj);
};

class ChunkStore {
 public:
  /*
   * store allocates the returned chunck,
   * caller need to release memory after use.
   */
  ChunkStore() = default;
  virtual ~ChunkStore() noexcept(false) {}

  virtual Chunk Get(const Hash& key) = 0;
  virtual bool Put(const Hash& key, const Chunk& chunk) = 0;
  virtual bool Exists(const Hash& key) = 0;
  virtual StoreInfo GetInfo() = 0;

  virtual StoreIterator begin() const = 0;
  virtual StoreIterator cbegin() const = 0;
  virtual StoreIterator end() const = 0;
  virtual StoreIterator cend() const = 0;
};

// wrap global functions inside a namespace
namespace store {

// have to be called before calling GetChunkStore(), otherwise has no effect
ChunkStore* InitChunkStore(const std::string& dir, const std::string& file,
                           bool persist);
ChunkStore* GetChunkStore();

}  // namespace store
}  // namespace ustore

#endif  // USTORE_STORE_CHUNK_STORE_H_
