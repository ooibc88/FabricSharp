// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CLIENT_H_
#define USTORE_CLUSTER_CLIENT_H_

#include <memory>
#include <string>
#include <vector>
#include "chunk/chunk.h"
#include "cluster/partitioner.h"
#include "cluster/response_blob.h"
#include "hash/hash.h"
#include "net/net.h"
#include "proto/messages.pb.h"
#include "types/ucell.h"
#include "store/chunk_store.h"

namespace ustore {

/**
 * Client is an abstracted class to parse response messages from ClientService
 * into app-level data structures.
 * It contains impls for parsing all return types of response messages.
 */

class Client {
 public:
  ~Client() = default;

 protected:
  explicit Client(ResponseBlob* blob)
    : id_(blob->id), net_(blob->net), res_blob_(blob) {}

  bool Send(UMessage* msg, const node_id_t& node_id) const;
  // helper methods for getting response
  ErrorCode GetEmptyResponse() const;
  ErrorCode GetVersionResponse(Hash* version) const;
  ErrorCode GetUCellResponse(UCell* value) const;
  ErrorCode GetStringListResponse(std::vector<string>* vals) const;
  ErrorCode GetVersionListResponse(std::vector<Hash>* versions) const;
  ErrorCode GetBoolResponse(bool* value) const;
  ErrorCode GetChunkResponse(Chunk* chunk) const;
  ErrorCode GetInfoResponse(std::vector<StoreInfo>* info) const;

 private:
  std::unique_ptr<UMessage> WaitForResponse() const;
  // send request to a node. Return false if there are
  // errors with network communication.

  const int id_ = 0;
  Net* const net_ = nullptr;  // for network communication
  ResponseBlob* const res_blob_ = nullptr;  // response blob
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CLIENT_H_
