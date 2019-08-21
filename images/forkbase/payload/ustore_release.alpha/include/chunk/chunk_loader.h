// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_LOADER_H_
#define USTORE_CHUNK_CHUNK_LOADER_H_

#include <unordered_map>
#include <string>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "store/chunk_store.h"
#include "utils/noncopyable.h"

namespace ustore {

class ChunkClient;
class DB;
class Partitioner;

// ChunkLoader is responsible to load chunks and cache them
class ChunkLoader : private Noncopyable {
 public:
  virtual ~ChunkLoader() = default;

  const Chunk* Load(const Hash& key);

 protected:
  ChunkLoader() = default;
  // Do real work for fetching a chunk, only called by Load()
  virtual Chunk GetChunk(const Hash& key) = 0;

  std::unordered_map<Hash, Chunk> cache_;
};

// Local chunk loader load chunks from local storage
class LocalChunkLoader : public ChunkLoader {
 public:
  LocalChunkLoader() : cs_(store::GetChunkStore()) {}
  ~LocalChunkLoader() = default;

 protected:
  Chunk GetChunk(const Hash& key) override;

 private:
  ChunkStore* const cs_;
};

// Partitioned chunk loader load chunks based on hash-based partitions
class PartitionedChunkLoader : public ChunkLoader {
 public:
  explicit PartitionedChunkLoader(const Partitioner* ptt, ChunkClient* client)
    : cs_(store::GetChunkStore()), ptt_(ptt), client_(client) {}
  ~PartitionedChunkLoader() = default;

 protected:
  Chunk GetChunk(const Hash& key) override;

 private:
  ChunkStore* const cs_;
  const Partitioner* const ptt_;
  ChunkClient* client_;
};

// Client chunk loader load chunks via client/worker service
class ClientChunkLoader : public ChunkLoader {
 public:
  // need a db instance
  ClientChunkLoader(DB* db, const Slice& key)
    : db_(db), key_(key.ToString()) {}
  // Delete all chunks
  ~ClientChunkLoader() = default;

 protected:
  Chunk GetChunk(const Hash& key) override;

 private:
  DB* const db_;
  std::string key_;
};

}  // namespace ustore

#endif  // USTORE_CHUNK_CHUNK_LOADER_H_
