// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_KVDB_DB_H_
#define USTORE_KVDB_DB_H_

#include <string>
#include <utility>
#include <vector>
#include <list>
#include <map>
#include "status.h"
#include "spec/object_db.h"
#include "spec/slice.h"
#include "worker/worker.h"
#include "store/chunk_store.h"
#include "utils/timer.h"

namespace ustore_kvdb {

#define ull unsigned long long

// class Iterator;
class HistReturn;
class BackwardReturn;
class ForwardReturn;


// a key-value store wrapper utilizing Blob data type.
class KVDB {
 public:
  explicit KVDB(ustore::WorkerID id = 42)
          noexcept : wk_(id, nullptr, true), odb_(&wk_) {
        this->base_= 2;
  }

  KVDB(const KVDB&) = delete;
  void operator=(const KVDB&) = delete;
  // Display the storage space for each type
  void OutputChunkStorage();

// API if Forkbase is used as a generic storage
  Status Get(const std::string& key, std::string* value);
  std::pair<Status, std::string> Get(const std::string& key);
  Status Put(const std::string& key, const std::string& value);

  // Iterate all the version value for the given key
  // NOTE: key = state identifier
  void IterateState(const std::string& key);

  // APIs for the storage and retrieval of the block, not used for Fabric v1.3
  std::pair<Status, std::string> GetBlock(const std::string& key,
                                          const std::string& version);

  std::pair<Status, std::string> PutBlock(const std::string& key,
                                          const std::string& value);

  // Init the global map, Expected to be called at the program start
  Status InitGlobalState();

  // Commit the block with the previous updated states
  std::pair<Status, std::string> Commit();

  // Update the state with the given block number and the dependent state identifiers
  bool PutState(const std::string& key, const std::string& val,
                const std::string& txnID,
                ull blk, const std::vector<std::string>& deps);
                
// Three relevant APIs for the historical or provenance query
//    default to be largetest ull to retrieve the latest value
  HistReturn Hist(const std::string& key, ull blk_idx=4294967295); 

  BackwardReturn Backward(const std::string& key, ull blk_idx); 

  ForwardReturn Forward(const std::string& key, ull blk_idx); 

  // Return the number of the latest block which includes a transaction that updates this key 
  ull GetLatestVersion(const std::string& key);
  
 private:
  // Return two vids, the former of which identifies the state entry updated immediately before or exactly at block blk_idx
// The latter identifiers the state entry immediately after block blk_idx
// If not found, return empty string
  std::pair<std::string, std::string> LowerUpperBoundVid(const std::string& key, ull blk_idx, ustore::ErrorCode* code);

  // Implementation of Algorithm 2 in Paper: Given the key and its updated blk_idx, 
  //   use parameter pointer to return the vid and blk number of the proceding entry in each level in DASL
  bool DASLAppend(const std::string& key, ull blk_idx, 
                  std::vector<ull>* pre_blks, 
                  std::vector<std::string>* pre_versions);


  static std::string MarshalProv(ull blk_idx, const std::string& txnID,
                              const std::vector<std::string>& for_keys,
                              const std::vector<std::string>& for_versions,
                              const std::vector<std::string>& dep_keys,
                              const std::vector<std::string>& dep_versions,
                              const std::vector<ull>& pre_blks,
                              const std::vector<std::string>& pre_versions, 
                              // Whether previous entry with the same key is anti-dependent on the current key
                              int self_dependent=0);


  static bool UnmarshalProv(std::string encoded,
                         ull* blk_idx,
                         std::string* txnID = nullptr,
                         std::vector<std::string>* for_keys = nullptr,
                         std::vector<std::string>* for_versions = nullptr,
                         std::vector<std::string>* dep_keys = nullptr,
                         std::vector<std::string>* dep_versions = nullptr,
                         std::vector<ull>* pre_blks = nullptr,
                         std::vector<std::string>* pre_versions = nullptr, 
                         int* self_dependent=nullptr);

  ustore::Worker wk_;
  ustore::ObjectDB odb_;

  ustore::VMap global_;
  // buffer forward dependency relationship in the current block
  std::map<std::string, std::string> temp_forward_key_vids_;

  // buffer the updated key with its returned vid in the current block
  std::map<std::string, std::string> updated_vids_; 
  ull base_ = 2;

  static const ustore::Slice GLOBAL_STATE_KEY;
  static const ustore::Slice DEFAULT_BRANCH;
};

class HistReturn {
 public:
  HistReturn() = default;
  HistReturn(Status stat, ull blk_idx, std::string value) : 
    stat_(stat), blk_idx_(blk_idx), value_(value) {}

  inline Status status() const {
    return stat_;
  }

  inline ull blk_idx() const {
    return blk_idx_;
  }

  inline std::string value() const {
    return value_;
  }
 private:
  Status stat_;
  ull blk_idx_;
  std::string value_;
};

class BackwardReturn {
 public:
  BackwardReturn() = default;
  BackwardReturn(Status stat, std::string txnID="")
    :stat_(stat), txnID_(txnID), dep_keys_(), dep_blk_idxs_() {}

  inline Status status() const {
    return stat_;
  }

  inline std::string txnID() const {
    return txnID_;
  }

  inline std::vector<std::string> dep_keys() {
    return dep_keys_;
  }

  inline std::vector<ull> dep_blk_idx() {
    return dep_blk_idxs_;
  }

  inline void AddKeyBlk(const std::string& key, ull blk_idx) {
    dep_keys_.push_back(key);
    dep_blk_idxs_.push_back(blk_idx);
  }
 private:
  Status stat_;
  std::string txnID_;
  std::vector<std::string> dep_keys_;
  std::vector<ull> dep_blk_idxs_;
};

class ForwardReturn {
 public:
  ForwardReturn() = default;
  ForwardReturn(Status stat)
    :stat_(stat), txnIDs_(), for_keys_(), for_blk_idxs_() {}

  inline Status status() const {
    return stat_;
  }

  inline std::vector<std::string> txnIDs() const {
    return txnIDs_;
  }

  inline std::vector<std::string> forward_keys() {
    return for_keys_;
  }

  inline std::vector<ull> forward_blk_idx() {
    return for_blk_idxs_;
  }

  inline void AddTxnIDKeyBlk(const std::string& txnID, const std::string& key, ull blk_idx) {
    txnIDs_.push_back(txnID);
    for_keys_.push_back(key);
    for_blk_idxs_.push_back(blk_idx);
  }
 private:
  Status stat_;
  std::vector<std::string> txnIDs_;
  std::vector<std::string> for_keys_;
  std::vector<ull> for_blk_idxs_;
};

}  // namespace ustore_kvdb

#endif  // USTORE_KVDB_DB_H_
