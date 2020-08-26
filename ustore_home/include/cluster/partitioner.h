// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_PARTITIONER_H_
#define USTORE_CLUSTER_PARTITIONER_H_
#include <functional>

#include <string>
#include <vector>
#include "cluster/port_helper.h"
#include "hash/hash.h"
#include "spec/slice.h"

namespace ustore {

/*
 * Partitioner is responsible for guiding the destination ip for each data
 * item.
 */
class Partitioner {
 public:
  virtual ~Partitioner() = default;

  // get dest id of a specific hash (For Chunked data types)
  inline int GetDestId(const Hash& hash) const {
    // no need to compute when having a single server
    if (dest_list_.size() == 1) return 0;
    uint64_t idx = *reinterpret_cast<const int64_t*>(hash.value() + 9);
    return idx % dest_list_.size();
  }
  // get dest id of a specific key (For UCell data type)
  inline int GetDestId(const Slice& key) const {
    return GetDestId(Hash::ComputeFrom(key.data(), key.len()));
  }
  // get dest addr of a specific hash (For Chunked data types)
  inline const std::string& GetDestAddr(const Hash& hash) const {
    return id2addr(GetDestId(hash));
  }
  // get dest addr of a specific key (For UCell data type)
  inline const std::string& GetDestAddr(const Slice& key) const {
    return id2addr(GetDestId(key));
  }

  // own id
  inline int id() const { return id_; }
  // own address
  inline const std::string& addr() const { return id2addr(id_); }
  // other addresses
  inline const std::vector<string> destAddrs() const { return dest_list_; }
  // id-address mapping
  inline const std::string& id2addr(int id) const {
    CHECK_GE(id, 0);
    return dest_list_[id];
  }

 protected:
  // Partitioner need to know the rule for getting final port
  Partitioner(const std::string& hostfile, const std::string& self_addr,
              std::function<std::string(std::string)> f_port);

 private:
  int id_ = -1;
  std::vector<std::string> dest_list_;
};

class WorkerPartitioner : public Partitioner {
 public:
  WorkerPartitioner(const std::string& hostfile, const std::string& self_addr)
      : Partitioner(hostfile, self_addr, PortHelper::WorkerPort) {}
  ~WorkerPartitioner() = default;
};

class ChunkPartitioner : public Partitioner {
 public:
  ChunkPartitioner(const std::string& hostfile, const std::string& self_addr)
      : Partitioner(hostfile, self_addr, PortHelper::ChunkPort) {}
  ~ChunkPartitioner() = default;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_PARTITIONER_H_
