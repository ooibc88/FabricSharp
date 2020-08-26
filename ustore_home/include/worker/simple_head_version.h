// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_SIMPLE_HEAD_VERSION_H_
#define USTORE_WORKER_SIMPLE_HEAD_VERSION_H_

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "worker/head_version.h"

namespace ustore {

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class SimpleHeadVersion : public HeadVersion {
 public:
  SimpleHeadVersion() = default;
  ~SimpleHeadVersion() = default;

  bool Load(const std::string& log_path) override;

  bool Dump(const std::string& log_path) override;

  bool GetBranch(const Slice& key, const Slice& branch,
                 Hash* ver) const override;

  std::vector<Hash> GetLatest(const Slice& key) const override;

  void PutBranch(const Slice& key, const Slice& branch,
                 const Hash& ver) override;

  void PutLatest(const Slice& key, const Hash& prev_ver1,
                 const Hash& prev_ver2, const Hash& ver) override;

  void RemoveBranch(const Slice& key, const Slice& branch) override;

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch) override;

  inline bool Exists(const Slice& key) const override {
    return latest_ver_.find(key) != latest_ver_.end();
  }

  bool Exists(const Slice& key, const Slice& branch) const override;

  bool IsLatest(const Slice& key, const Hash& ver) const override;

  inline bool IsBranchHead(const Slice& key, const Slice& branch,
                           const Hash& ver) const override {
    return Exists(key, branch) ? branch_ver_.at(key).at(branch) == ver : false;
  }

  std::vector<std::string> ListKey() const override;

  std::vector<std::string> ListBranch(const Slice& key) const override;

  /**
   * linqian: remove this since branchVersion() is unused.
   */
  inline const std::unordered_map<PSlice, std::map<PSlice, Hash>>&
      branchVersion() const {
    return branch_ver_;
  }

 private:
  // use std::map for branch to preserve branch order
  std::unordered_map<PSlice, std::map<PSlice, Hash>> branch_ver_;
  std::unordered_map<PSlice, std::unordered_set<Hash>> latest_ver_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_SIMPLE_HEAD_VERSION_H_
