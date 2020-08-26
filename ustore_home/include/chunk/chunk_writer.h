// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_WRITER_H_
#define USTORE_CHUNK_CHUNK_WRITER_H_

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"
#include "utils/noncopyable.h"

namespace ustore {

class ChunkClient;
class Partitioner;

// ChunkWriter is responsible to persist chunks into storage
class ChunkWriter : private Noncopyable {
 public:
  virtual ~ChunkWriter() = default;

  virtual bool Write(const Hash& key, const Chunk& chunk) = 0;

 protected:
  ChunkWriter() = default;
};

// Local chunk writer write chunks to local storage
class LocalChunkWriter : public ChunkWriter {
 public:
  LocalChunkWriter() : cs_(store::GetChunkStore()) {}
  ~LocalChunkWriter() = default;

  bool Write(const Hash& key, const Chunk& chunk) override;

 private:
  ChunkStore* const cs_;
};

// Partitioned chunk loader write chunks based on hash-based partitions
class PartitionedChunkWriter : public ChunkWriter {
 public:
  explicit PartitionedChunkWriter(const Partitioner* ptt, ChunkClient* client)
    : cs_(store::GetChunkStore()), ptt_(ptt), client_(client) {}
  ~PartitionedChunkWriter() = default;

  bool Write(const Hash& key, const Chunk& chunk) override;

 private:
  ChunkStore* const cs_;
  const Partitioner* ptt_;
  ChunkClient* client_;
};

}  // namespace ustore

#endif  // USTORE_CHUNK_CHUNK_WRITER_H_
