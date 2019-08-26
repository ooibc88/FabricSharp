// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_WORKER_CLIENT_H_
#define USTORE_CLUSTER_WORKER_CLIENT_H_

#include <string>
#include <vector>
#include "cluster/chunk_client_service.h"
#include "cluster/client.h"
#include "cluster/partitioner.h"
#include "cluster/response_blob.h"
#include "proto/messages.pb.h"
#include "spec/db.h"

namespace ustore {

/**
 * WorkerClient object is created from RemoteClientService.
 *
 * Main entrance to the storage. It interfaces with the client (same process),
 * the master and the worker. It has 3 main tasks:
 *
 * 1. Maintain a list of worker, which is synced with the master.
 * 2. Contain storage APIs to be invoked by the client. For each method it
 * forwards the corresponding request to the appropriate worker, then waits
 * for a response.
 * 3. When the response indicates error (INVALID_RANGE, for example), it
 * syncs with the master.
 *
 * Each WorkerClient can be run on a separate thread, processing requests synchronously.
 * Responses arrive asynchronously from the network. To route the response back to the
 * correct thread, we use thread ID to identify the message. That is:
 *    + Each UMessage request now contains a field named "source"
 *    + After sending the message, the thread waits on a ResponseBlob
 *      (associated with this "source")
 *    + When a UMessage response arrives, the network thread (callback)
 *      checks for the source and wakes up the corresponding ResponseBlob
 *
 */

class WorkerClient : public Client, public DB {
 public:
  WorkerClient(ResponseBlob* blob, const Partitioner* ptt,
      ChunkClientService* ck_svc) : Client(blob), ptt_(ptt) {
    if (ck_svc) ck_cli_.push_back(ck_svc->CreateChunkClient());
  }

  ~WorkerClient() = default;

  // Storage APIs. Inheritted from DB.
  ErrorCode Get(const Slice& key, const Slice& branch, UCell* meta) const
    override;
  ErrorCode Get(const Slice& key, const Hash& version,
                UCell* meta) const override;

  ErrorCode Put(const Slice& key, const Value& value,
                const Slice& branch, Hash* version) override;
  ErrorCode Put(const Slice& key, const Value& value,
                const Hash& pre_version, Hash* version) override;

  ErrorCode Merge(const Slice& key, const Value& value,
                  const Slice& tgt_branch, const Slice& ref_branch,
                  Hash* version) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                  const Slice& tgt_branch, const Hash& ref_version,
                  Hash* version) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                  const Hash& ref_version1, const Hash& ref_version2,
                  Hash* version) override;

  ErrorCode ListKeys(std::vector<std::string>* keys) const override;
  ErrorCode ListBranches(const Slice& key,
                         std::vector<std::string>* branches) const override;

  ErrorCode Exists(const Slice& key, bool* exist) const override;
  ErrorCode Exists(const Slice& key, const Slice& branch, bool* exist) const
    override;

  ErrorCode GetBranchHead(const Slice& key, const Slice& branch,
                          Hash* version) const override;
  ErrorCode IsBranchHead(const Slice& key, const Slice& branch,
                         const Hash& version, bool* isHead) const override;

  ErrorCode GetLatestVersions(const Slice& key,
                              std::vector<Hash>* versions) const override;
  ErrorCode IsLatestVersion(const Slice& key, const Hash& version,
                            bool* isLatest) const override;

  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch) override;
  ErrorCode Branch(const Slice& key, const Hash& version,
                   const Slice& new_branch) override;
  ErrorCode Rename(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch) override;
  ErrorCode Delete(const Slice& key, const Slice& branch) override;

  ErrorCode GetChunk(const Slice& key, const Hash& version,
                     Chunk* chunk) const override;

  ErrorCode GetStorageInfo(std::vector<StoreInfo>* info) const override;

 protected:
  void CreatePutMessage(const Slice& key, const Value& value, UMessage* msg)
      const;
  void CreateGetMessage(const Slice& key, UMessage* msg) const;
  void CreateBranchMessage(const Slice& key, const Slice& new_branch,
      UMessage* msg) const;
  void CreateMergeMessage(const Slice& key, const Value& value, UMessage* msg)
      const;

  const Partitioner* const ptt_;  // partitioner to route destination worker
  std::vector<ChunkClient> ck_cli_;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_WORKER_CLIENT_H_
