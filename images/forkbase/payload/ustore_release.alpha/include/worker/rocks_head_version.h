// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_ROCKS_HEAD_VERSION_H_
#define USTORE_WORKER_ROCKS_HEAD_VERSION_H_

#include <memory>
#include <string>
#include <vector>
#include "utils/rocksdb.h"
#include "worker/head_version.h"

namespace ustore {

class RocksBranchVersionDB : public RocksDB {
 public:
  RocksBranchVersionDB();
  explicit RocksBranchVersionDB(const std::shared_ptr<rocksdb::Cache>& cache);
  ~RocksBranchVersionDB() = default;

  bool Get(const Slice& key, const Slice& branch, Hash* ver) const;

  bool Exists(const Slice& key, const Slice& branch) const;

  bool Put(const Slice& key, const Slice& branch,
           const rocksdb::Slice& ver);

  bool Delete(const Slice& key, const Slice& branch);

  bool Move(const Slice& key, const Slice& src, const Slice& dst);

  std::vector<std::string> GetBranches(const Slice& key) const;

 private:
  std::string DBKey(const Slice& key, const Slice& branch) const;
  std::string ExtractBranch(const rocksdb::Slice& db_key) const;

  const rocksdb::SliceTransform* NewPrefixTransform() const override;
};

class RocksLatestVersionDB : public RocksDB {
 public:
  RocksLatestVersionDB();
  explicit RocksLatestVersionDB(const std::shared_ptr<rocksdb::Cache>& cache);
  ~RocksLatestVersionDB() = default;

  std::vector<Hash> Get(const Slice& key) const;

  bool Exists(const Slice& key) const;

  bool Exists(const Slice& key, const Hash& ver) const;

  bool Merge(const Slice& key, const Hash& prev_ver1, const Hash& prev_ver2,
             const Hash& ver);

  std::vector<std::string> GetKeys() const;

 private:
  rocksdb::MergeOperator* NewMergeOperator() const override;
};

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class RocksHeadVersion : public HeadVersion {
 public:
  RocksHeadVersion();
  ~RocksHeadVersion() = default;

  void CloseDB();

  static bool DestroyDB(const std::string& db_path);

  bool DestroyDB();

  bool Load(const std::string& db_path) override;

  bool Dump(const std::string& db_path) override;

  inline bool GetBranch(const Slice& key, const Slice& branch,
                        Hash* ver) const override {
    return branch_db_.Get(key, branch, ver);
  }

  inline std::vector<Hash> GetLatest(const Slice& key) const override {
    return latest_db_.Get(key);
  }

  void PutBranch(const Slice& key, const Slice& branch,
                 const Hash& ver) override;

  void PutLatest(const Slice& key, const Hash& prev_ver1,
                 const Hash& prev_ver2, const Hash& ver) override;

  void RemoveBranch(const Slice& key, const Slice& branch) override;

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch) override;

  inline bool Exists(const Slice& key) const override {
    return latest_db_.Exists(key);
  }

  inline bool Exists(const Slice& key, const Slice& branch) const override {
    return branch_db_.Exists(key, branch);
  }

  inline bool IsLatest(const Slice& key, const Hash& ver) const override {
    return latest_db_.Exists(key, ver);
  }

  inline bool IsBranchHead(const Slice& key, const Slice& branch,
                           const Hash& ver) const override {
    Hash head;
    return GetBranch(key, branch, &head) ? head == ver : false;
  }

  inline std::vector<std::string> ListKey() const override {
    return latest_db_.GetKeys();
  }

  inline std::vector<std::string> ListBranch(const Slice& key) const override {
    return branch_db_.GetBranches(key);
  }

 private:
  std::shared_ptr<rocksdb::Cache> db_shared_cache_;
  RocksBranchVersionDB branch_db_;
  RocksLatestVersionDB latest_db_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_ROCKS_HEAD_VERSION_H_
