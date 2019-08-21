// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_WORKER_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_WORKER_CLIENT_SERVICE_H_

#include <memory>
#include <vector>
#include "cluster/chunk_client_service.h"
#include "cluster/client_service.h"
#include "cluster/worker_client.h"
#include "cluster/partitioner.h"
#include "utils/env.h"

namespace ustore {


/**
 * Service that handle remote client requests to the database.
 * Multiple clients/threads share the same service, thus avoiding
 * creating one connection for each client.
 *
 * To interact with the database, the user first creates a ClientDb
 * object via CreateClientDb().
 *
 * The database servers (Worker) knows about every RemoteClientService
 * in the system, and can send responses asynchronously.
 */
class WorkerClientService : public ClientService {
 public:
  WorkerClientService()
    : ClientService(&ptt_), ptt_(Env::Instance()->config().worker_file(), "") {
    // only need chunk client when want to get chunk bypass worker
    if (Env::Instance()->config().get_chunk_bypass_worker())
      ck_svc_.reset(new ChunkClientService());
  }
  ~WorkerClientService() = default;

  void Init() override;
  /**
   * Create a new ClientDb connecting to the database.
   * Interaction with the database is through this object.
   */
  WorkerClient CreateWorkerClient();

 private:
  const WorkerPartitioner ptt_;
  std::unique_ptr<ChunkClientService> ck_svc_;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_WORKER_CLIENT_SERVICE_H_
