// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CHUNK_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_CHUNK_CLIENT_SERVICE_H_

#include "cluster/chunk_client.h"
#include "cluster/client_service.h"
#include "cluster/partitioner.h"
#include "utils/env.h"

namespace ustore {

class ChunkClientService : public ClientService {
 public:
  ChunkClientService()
    : ClientService(&ptt_),
      ptt_(Env::Instance()->config().worker_file(), "") {}
  ~ChunkClientService() = default;

  void Init() override;
  ChunkClient CreateChunkClient();

 private:
  const ChunkPartitioner ptt_;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CHUNK_CLIENT_SERVICE_H_
