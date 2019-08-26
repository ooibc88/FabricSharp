// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_MASTER_SERVICE_H_
#define USTORE_CLUSTER_MASTER_SERVICE_H_

#include <string>
#include "cluster/service.h"
#include "net/net.h"

namespace ustore {

/**
 * The MasterService receives requests from ClientService about key
 * range changes (RangeInfo). It then invokes Master to return
 * the ranges.
 * Basically a simplified version of WorkerService.
 */
class MasterService : public Service {
 public:
  MasterService(const node_id_t& id, const string& config_path)
      : Service(id), config_path_(config_path) {}
  ~MasterService();

  /**
   * Handle requests:
   * 1. It parse msg into a message (RangeRequest, e.g)
   * 2. Invoke the processing logic from Master.
   * 3. Construct a response (RangeResponse, e.g.) and send back.
   */
  void HandleRequest(const void *msg, int size, const node_id_t& source)
    override;

 private:
  string config_path_;  // the master may read from a global config file
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_MASTER_SERVICE_H_
