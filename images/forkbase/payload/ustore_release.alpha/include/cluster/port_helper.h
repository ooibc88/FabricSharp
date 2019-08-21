// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_PORT_HELPER_H_
#define USTORE_CLUSTER_PORT_HELPER_H_

#include <string>

namespace ustore {

/*
 * Router is responsible for calculate corresponding port for a service
 */
class PortHelper {
 public:
  // worker service port is even
  static std::string WorkerPort(std::string addr) {
    if (addr.back() & 1) addr.back() ^= 1;
    return addr;
  }
  // chunk service port is odd
  static std::string ChunkPort(std::string addr) {
    if (!(addr.back() & 1)) addr.back() ^= 1;
    return addr;
  }
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_PORT_HELPER_H_
