// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_BLOB_STORE_H_
#define USTORE_SPEC_BLOB_STORE_H_

#define __BLOB_STORE_USE_MAP_MULTI_SET_OP__

#include <boost/filesystem.hpp>
#include <string>
#include <vector>
#include "spec/object_db.h"
#include "utils/utils.h"

namespace ustore {

using Dataset = VMap;
using DataEntry = VBlob;

class BlobStore {
 public:
  explicit BlobStore(DB* db) noexcept : odb_(db) {}
  ~BlobStore() = default;

  ErrorCode ListDataset(std::vector<std::string>* datasets);

  ErrorCode ExistsDataset(const std::string& ds_name, bool* exists);

  ErrorCode ExistsDataset(const std::string& ds_name,
                          const std::string& branch, bool* exists);

  ErrorCode CreateDataset(const std::string& ds_name,
                          const std::string& branch);

  ErrorCode GetDataset(const std::string& ds_name, const std::string& branch,
                       Dataset* ds);

  ErrorCode ExportDatasetBinary(const std::string& ds_name,
                                const std::string& branch,
                                const boost::filesystem::path& file_path,
                                size_t* n_entries, size_t* n_bytes);

  inline ErrorCode ExportDatasetBinary(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& file_path) {
    size_t n_entries, n_bytes;
    return ExportDatasetBinary(
             ds_name, branch, file_path, &n_entries, &n_bytes);
  }

  ErrorCode BranchDataset(const std::string& ds_name,
                          const std::string& old_branch,
                          const std::string& new_branch);

  ErrorCode ListDatasetBranch(const std::string& ds_name,
                              std::vector<std::string>* branches);

  ErrorCode DiffDataset(const std::string& lhs_ds_name,
                        const std::string& lhs_branch,
                        const std::string& rhs_ds_name,
                        const std::string& rhs_branch,
                        std::vector<std::string>* diff_keys);

  ErrorCode DeleteDataset(const std::string& ds_name,
                          const std::string& branch);

  ErrorCode ExistsDataEntry(const std::string& ds_name,
                            const std::string& entry_name, bool* exists);

  ErrorCode ExistsDataEntry(const std::string& ds_name,
                            const std::string& branch,
                            const std::string& entry_name, bool* exists);

  ErrorCode GetDataEntry(const std::string& ds_name,
                         const std::string& branch,
                         const std::string& entry_name, DataEntry* entry);

  ErrorCode GetDataEntryBatch(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& dir_path,
                              size_t* n_entries, size_t* n_bytes);

  inline ErrorCode GetDataEntryBatch(const std::string& ds_name,
                                     const std::string& branch,
                                     const boost::filesystem::path& dir_path) {
    size_t n_entries, n_bytes;
    return GetDataEntryBatch(ds_name, branch, dir_path, &n_entries, &n_bytes);
  }

  ErrorCode PutDataEntry(const std::string& ds_name,
                         const std::string& branch,
                         const std::string& entry_name,
                         const std::string& entry_val,
                         Hash* entry_ver);

  inline ErrorCode PutDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name,
                                const std::string& entry_val) {
    Hash entry_ver;
    return PutDataEntry(ds_name, branch, entry_name, entry_val, &entry_ver);
  }

  ErrorCode PutDataEntryBatch(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& dir_path,
                              size_t* n_entries, size_t* n_bytes);

  inline ErrorCode PutDataEntryBatch(const std::string& ds_name,
                                     const std::string& branch,
                                     const boost::filesystem::path& dir_path) {
    size_t n_entries, n_bytes;
    return PutDataEntryBatch(ds_name, branch, dir_path, &n_entries, &n_bytes);
  }

  ErrorCode DeleteDataEntry(const std::string& ds_name,
                            const std::string& branch,
                            const std::string& entry_name);

  ErrorCode ListDataEntryBranch(const std::string& ds_name,
                                const std::string& entry_name,
                                std::vector<std::string>* branches);

  template<class T1, class T2>
  static inline std::string GlobalKey(const T1& ds_name,
                                      const T2& entry_name) {
    return Utils::ToString(ds_name) + "::" + Utils::ToString(entry_name);
  }

 private:
  ErrorCode GetDatasetList(VSet* ds_list);

  ErrorCode UpdateDatasetList(const Slice& ds_name, bool to_delete = false);

  ErrorCode ReadDataset(const Slice& ds_name, const Slice& branch, Dataset* ds);

  ErrorCode ReadDataEntryHash(const std::string& ds_name,
                              const std::string& entry_name,
                              const Hash& entry_ver,
                              Hash* entry_hash);

  ErrorCode ReadDataEntry(const std::string& ds_name,
                          const std::string& entry_name, const Hash& entry_ver,
                          DataEntry* entry_val);

  ErrorCode WriteDataEntry(const std::string& ds_name,
                           const std::string& entry_name,
                           const std::string& entry_val,
                           const Hash& prev_entry_ver,
                           Hash* entry_ver);

  ObjectDB odb_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_BLOB_STORE_H_
