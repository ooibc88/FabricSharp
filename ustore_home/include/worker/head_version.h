// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_HEAD_VERSION_H_
#define USTORE_WORKER_HEAD_VERSION_H_

#include <string>
#include <vector>
#include "hash/hash.h"
#include "spec/slice.h"
#include "utils/noncopyable.h"

namespace ustore {

class HeadVersion : private Noncopyable {
 public:
  // TODO(yaochang): persist the log of branch update.
  virtual void LogBranchUpdate(const Slice& key, const Slice& branch,
                               const Hash& ver) const {}

  virtual bool Load(const std::string& persist_path) = 0;

  virtual bool Dump(const std::string& persist_path) = 0;

  virtual bool GetBranch(const Slice& key, const Slice& branch,
                         Hash* ver) const = 0;

  virtual std::vector<Hash> GetLatest(const Slice& key) const = 0;

  virtual void PutBranch(const Slice& key, const Slice& branch,
                         const Hash& ver) = 0;

  virtual void PutLatest(const Slice& key, const Hash& prev_ver1,
                         const Hash& prev_ver2, const Hash& ver) = 0;

  virtual void RemoveBranch(const Slice& key, const Slice& branch) = 0;

  virtual void RenameBranch(const Slice& key, const Slice& old_branch,
                            const Slice& new_branch) = 0;

  virtual bool Exists(const Slice& key) const = 0;

  virtual bool Exists(const Slice& key, const Slice& branch) const = 0;

  virtual bool IsLatest(const Slice& key, const Hash& ver) const = 0;

  virtual bool IsBranchHead(const Slice& key, const Slice& branch,
                            const Hash& ver) const = 0;

  virtual std::vector<std::string> ListKey() const = 0;

  virtual std::vector<std::string> ListBranch(const Slice& key) const = 0;
};

}  // namespace ustore

#endif  // USTORE_WORKER_HEAD_VERSION_H_
