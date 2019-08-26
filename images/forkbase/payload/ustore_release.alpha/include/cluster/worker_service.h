// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_WORKER_SERVICE_H_
#define USTORE_CLUSTER_WORKER_SERVICE_H_

#include <memory>
#include <mutex>
#include "cluster/access_logging.h"
#include "cluster/chunk_service.h"
#include "cluster/host_service.h"
#include "cluster/partitioner.h"
#include "cluster/port_helper.h"
#include "proto/messages.pb.h"
#include "utils/env.h"
#include "worker/worker.h"

namespace ustore {

/**
 * The WorkerService receives requests from WorkerClientService and invokes
 * the Worker to process the message.
 */
class WorkerService : public HostService {
 public:
  WorkerService(const node_id_t& addr, bool persist)
    : HostService(PortHelper::WorkerPort(addr)),
      ptt_(Env::Instance()->config().worker_file(), addr),
      worker_(ptt_.id(),
              Env::Instance()->config().enable_dist_store() ? &ptt_ : nullptr,
              persist),
      access_(Env::Instance()->config().access_log_dir(), addr) {
      auto& config = Env::Instance()->config();
      // only need chunk service when other worker or client need it
      if (config.enable_dist_store() || config.get_chunk_bypass_worker())
        ck_svc_.reset(new ChunkService(addr));
    }
  ~WorkerService() = default;

  void Init() override;
  void HandleRequest(const void *msg, int size, const node_id_t& source)
    override;

 private:
  Value ValueFromRequest(const ValuePayload& payload);
  void HandlePutRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleMergeRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleListRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleExistsRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetBranchHeadRequest(const UMessage& umsg,
                                  ResponsePayload* response);
  void HandleIsBranchHeadRequest(const UMessage& umsg,
                                 ResponsePayload* response);
  void HandleGetLatestVersionRequest(const UMessage& umsg,
                                     ResponsePayload* response);
  void HandleIsLatestVersionRequest(const UMessage& umsg,
                                    ResponsePayload* response);
  void HandleBranchRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleRenameRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleDeleteRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetChunkRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetInfoRequest(const UMessage& umsg, UMessage* response);

  const ChunkPartitioner ptt_;
  Worker worker_;  // where the logic happens
  std::mutex lock_;
  std::unique_ptr<ChunkService> ck_svc_;
  AccessLogging access_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_WORKER_SERVICE_H_
