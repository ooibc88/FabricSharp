// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_DB_H_
#define USTORE_SPEC_DB_H_

#include <string>
#include <vector>

#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "store/chunk_store.h"
#include "types/type.h"
#include "types/ucell.h"

namespace ustore {

class DB {
 public:
  /**
   * @brief Read the value which is the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Get(const Slice& key, const Slice& branch,
                        UCell* meta) const = 0;
  /**
   * @brief Read the value of a version.
   *
   * @param key     Target key.
   * @param versin  Version to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Get(const Slice& key, const Hash& version,
                        UCell* meta) const = 0;
  /**
   * @brief Write a new value as the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to update.
   * @param value   Value to write.
   * @param version Returned version.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value& value,
                        const Slice& branch, Hash* version) = 0;
  /**
   * @brief Write a new value as the successor of a version.
   *
   * @param key         Target key.
   * @param pre_version Previous version refered to.
   * @param value       Value to write.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value& value,
                        const Hash& pre_version, Hash* version) = 0;
  /**
   * @brief Merge target branch to a referring branch.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_branch  The referring branch.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) = 0;
  /**
   * @brief Merge target branch to a referring version.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_version The referring version.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) = 0;
  /**
   * @brief Merge two existing versions.
   *
   * @param key           Target key.
   * @param ref_version1  The first referring branch.
   * @param ref_version2  The second referring version.
   * @param value         (Optional) use if cannot auto-resolve conflicts.
   * @param version       Returned version.
   * @return              Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) = 0;
  /**
   * @brief List all keys.
   *
   * @param keys  Returned set of keys.
   * @return      Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode ListKeys(std::vector<std::string>* versions) const = 0;
  /**
   * @brief List all branches of the specified key.
   *
   * @param keys      Target key.
   * @param branches  Returned set of branches.
   * @return          Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode ListBranches(const Slice& key,
                                 std::vector<std::string>* branches) const = 0;
  /**
   * @brief Check for the existence of the specified key.
   *
   * @param key    Requested key.
   * @param exist  True if the specified key exists.
   * @return       Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Exists(const Slice& key, bool* exist) const = 0;
  /**
   * @brief Check for the existence of the specified branch.
   *
   * @param key     Target key.
   * @param branch  Requested branch.
   * @param exist   True if the specified branch exists.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Exists(const Slice& key, const Slice& branch,
                           bool* exist) const = 0;
  /**
   * @brief Obtain the head version of the specified branch.
   *
   * @param key      Target key.
   * @param branch   Target branch.
   * @param version  Returned head version of the branch; Hash::kNull if the
   *                 requesting head version is unavailable.
   * @return         Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode GetBranchHead(const Slice& key, const Slice& branch,
                                  Hash* version) const = 0;
  /**
   * @brief Check whether the given version is the head version of the
   *        specified branch.
   *
   * @param key      Target key.
   * @param branch   Target branch.
   * @param version  Requested version.
   * @param isHead   True if the given version is the head.
   * @return         Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode IsBranchHead(const Slice& key, const Slice& branch,
                                 const Hash& version, bool* isHead) const = 0;
  /**
   * @brief Obtain all latest versions of the specified key.
   *
   * @param key       Target key.
   * @param versions  Returned set of latest versions.
   * @return          Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode GetLatestVersions(const Slice& key,
                                      std::vector<Hash>* versions) const = 0;
  /**
   * @brief Check whether the given version is the head version of the
   *        specified branch.
   *
   * @param key       Target key.
   * @param version   Requested version.
   * @param isLatest  True if the given version is the latest.
   * @return          Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode IsLatestVersion(const Slice& key, const Hash& version,
                                    bool* isLatest) const = 0;
  /**
   * @brief Create a new branch which points to the head of a branch.
   *
   * @param key         Target key.
   * @param old_branch  Existing branch.
   * @param new_branch  New branch.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) = 0;
  /**
   * @brief Create a new branch which points to an existing version.
   *
   * @param key         Target key.
   * @param version     Existing version.
   * @param new_branch  New branch.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Hash& version,
                           const Slice& new_branch) = 0;
  /**
   * @brief Rename an existing branch.
   *
   * @param key         Target key.
   * @param old_branch  Existing branch name.
   * @param new_branch  New branch name.
   *                    Remove the branch if new_branch = "".
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Rename(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) = 0;
  /**
   * @brief Delete the branch.
   *
   * @param key Data key.
   * @param branch The to-be-deleted branch.
   * @return Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Delete(const Slice& key, const Slice& branch) = 0;
  /**
   * @brief Read a chunk from a chunk id.
   *
   * TODO(wangsh): remove key later, when chunk store is partitioned by hash
   * @param key     Target key.
   * @param versin  Version to read.
   * @param chunk   Returned chunk.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode GetChunk(const Slice& key, const Hash& version,
                             Chunk* chunk) const = 0;
  /**
   * @brief Read storage information.
   *
   * @param info    Returned storage information.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode GetStorageInfo(std::vector<StoreInfo>* info) const = 0;

 protected:
  DB() = default;
  virtual ~DB() = default;
};

}  // namespace ustore

#endif  // USTORE_SPEC_DB_H_
