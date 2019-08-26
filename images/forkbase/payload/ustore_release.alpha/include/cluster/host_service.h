// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_HOST_SERVICE_H_
#define USTORE_CLUSTER_HOST_SERVICE_H_

#include <memory>
#include <string>
#include <utility>
#include "cluster/service.h"
#include "net/net.h"
#include "utils/env.h"

namespace ustore {

/**
 * HostService is an abstracted class to handle request from server side.
 * A HostService receives requests from Client and invokes corresponding
 * classes to process the message.
 * Derived class from HostService should provide impl for handling requests.
 */
class HostService : public Service {
 public:
  // use another port if xor_port = true
  explicit HostService(const node_id_t& addr) : node_addr_(addr) {}
  ~HostService() = default;

  /**
   * Handle requests:
   * 1. It parse msg into a UStoreMessage
   * 2. Invoke the processing logic correspondingly.
   * 3. Construct a response and send back to source.
   */
  virtual void HandleRequest(const void *msg, int size,
                             const node_id_t& source) = 0;

 protected:
  inline void Init(std::unique_ptr<CallBack> callback) {
    Net* net = net::CreateServerNetwork(node_addr_,
               Env::Instance()->config().recv_threads());
    Service::Init(std::unique_ptr<Net>(net), std::move(callback));
  }
  inline void Send(const node_id_t& source, byte_t* ptr, int len) {
    net_->GetNetContext(source)->Send(ptr, static_cast<size_t>(len));
  }

  node_id_t node_addr_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_HOST_SERVICE_H_
