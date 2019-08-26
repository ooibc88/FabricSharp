// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_RESPONSE_BLOB_H_
#define USTORE_CLUSTER_RESPONSE_BLOB_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include "net/net.h"
#include "proto/messages.pb.h"

namespace ustore {

using google::protobuf::Message;

/**
 * A unit on the response queue. Each client request thread
 * waits on one of this object. The thread goes to sleep waiting for has_msg
 * condition to hold true. The has_msg variable will be set by the registered
 * callback method of the network thread
 *
 * This object is used under the assumption that the client issues requests in
 * a synchronous manner. As a result, the msg must be cleared before another
 * response is set.
 */
struct ResponseBlob {
  int id;
  Net* net = nullptr;
  std::mutex lock;
  std::condition_variable condition;

  bool has_msg;
  // message will be takeover by corresponding client, no memory leak here
  Message* message = nullptr;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_RESPONSE_BLOB_H_
