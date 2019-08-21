// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CHUNK_CLIENT_H_
#define USTORE_CLUSTER_CHUNK_CLIENT_H_

#include "cluster/client.h"

namespace ustore {


/**
 * The client part of chunk service, providing APIs for fetching, writing and checking
 * certain chunk hash.
 * An extension of ClientDB
 */

class ChunkClient : public Client {
 public:
  ChunkClient(ResponseBlob* blob, const Partitioner* ptt)
    : Client(blob), ptt_(ptt) {}
  ~ChunkClient() = default;

  ErrorCode Get(const Hash& hash, Chunk* chunk) const;
  ErrorCode Put(const Hash& hash, const Chunk& chunk);
  ErrorCode Exists(const Hash& hash, bool* exist) const;

  // only used by worker client while enable_dist_store = false
  ErrorCode Get(const Slice& key, const Hash& hash, Chunk* chunk) const;

 private:
  void CreateChunkMessage(const Hash& hash, UMessage *msg) const;

  const Partitioner* const ptt_;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CHUNK_CLIENT_H_
