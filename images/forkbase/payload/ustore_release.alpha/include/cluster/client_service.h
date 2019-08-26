// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_CLIENT_SERVICE_H_

#include <memory>
#include <vector>
#include "cluster/partitioner.h"
#include "cluster/response_blob.h"
#include "cluster/service.h"
#include "net/net.h"

namespace ustore {

/**
 * ClientService is an abstracted class to handle response from server.
 * A ClientService receives responses from Server and invokes corresponding
 * classes to process the message.
 */
class ClientService : public Service {
 public:
  explicit ClientService(const Partitioner* ptt)
    : nclients_(0), ptt_(ptt) {}
  virtual ~ClientService() = default;

  void Init(std::unique_ptr<CallBack> callback);
  void HandleResponse(const void *msg, int size, const node_id_t& source);

 protected:
  ResponseBlob* CreateResponseBlob();

 private:
  int nclients_;  // how many RequestHandler thread it uses
  const Partitioner* const ptt_;
  std::vector<std::unique_ptr<ResponseBlob>> responses_;  // the response queue
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_CLIENT_SERVICE_H_
