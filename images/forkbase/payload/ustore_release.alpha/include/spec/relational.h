// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_RELATIONAL_H_
#define USTORE_SPEC_RELATIONAL_H_

#define __FAST_STRING_TOKENIZE__
// #define __MOCK_FLUSH__  // for bottleneck test only
// #define __DELAY_COUNT_ROWS_IN_MS__ 2500
// #define __DELAY_BATCH_PROC_IN_MS__ 250

#include <atomic>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "spec/object_db.h"
#include "utils/blocking_queue.h"
#include "utils/utils.h"

namespace ustore {

using Table = VMap;
using Column = VList;
using Row = std::unordered_map<std::string, std::string>;
using TableDiffIterator = DuallyDiffKeyIterator;
using ColumnDiffIterator = DuallyDiffIndexIterator;

// for experiment only
using FuncRow = std::unordered_map<std::string, std::function<std::string()>>;

class ColumnStore {
 public:
  explicit ColumnStore(DB* db) noexcept : odb_(db) {}
  ~ColumnStore() = default;

  ErrorCode ExistsTable(const std::string& table_name, bool* exists);

  ErrorCode ExistsTable(const std::string& table_name,
                        const std::string& branch_name, bool* exists);

  ErrorCode CreateTable(const std::string& table_name,
                        const std::string& branch_name);

  ErrorCode LoadCSV(const std::string& file_path,
                    const std::string& table_name,
                    const std::string& branch_name,
                    size_t batch_size = 5000, bool print_progress = true);

  ErrorCode DumpCSV(const std::string& file_path,
                    const std::string& table_name,
                    const std::string& branch_name);

  ErrorCode GetTable(const std::string& table_name,
                     const std::string& branch_name, Table* tab);

  ErrorCode GetTableSchema(const std::string& table_name,
                           const std::string& branch_name, Row* schema);

  ErrorCode GetTableSize(const std::string& table_name,
                         const std::string& branch_name, long* tab_sz);

  ErrorCode BranchTable(const std::string& table_name,
                        const std::string& old_branch_name,
                        const std::string& new_branch_name);

  ErrorCode ListTableBranch(const std::string& table_name,
                            std::vector<std::string>* branches);

  inline TableDiffIterator DiffTable(const Table& lhs, const Table& rhs) {
    return UMap::DuallyDiff(lhs, rhs);
  }

  ErrorCode MergeTable(const std::string& table_name,
                       const std::string& tgt_branch_name,
                       const std::string& ref_branch_name,
                       const std::string& remove_col_name);

  ErrorCode MergeTable(const std::string& table_name,
                       const std::string& tgt_branch_name,
                       const std::string& ref_branch_name,
                       const std::string& new_col_name,
                       const std::vector<std::string>& new_col_vals);

  ErrorCode DeleteTable(const std::string& table_name,
                        const std::string& branch_name);

  ErrorCode ExistsColumn(const std::string& table_name,
                         const std::string& col_name, bool* exists);

  ErrorCode ExistsColumn(const std::string& table_name,
                         const std::string& branch_name,
                         const std::string& col_name, bool* exists);

  ErrorCode GetColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name, Column* col);

  ErrorCode PutColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name,
                      const std::vector<std::string>& col_vals);

  ErrorCode ListColumnBranch(const std::string& table_name,
                             const std::string& col_name,
                             std::vector<std::string>* branches);

  ErrorCode DeleteColumn(const std::string& table_name,
                         const std::string& branch_name,
                         const std::string& col_name);

  ErrorCode ExistsRow(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& ref_col_name,
                      const std::string& ref_val,
                      bool* exists);

  ErrorCode GetRow(const std::string& table_name,
                   const std::string& branch_name,
                   const std::string& ref_col_name,
                   const std::string& ref_val,
                   std::unordered_map<size_t, Row>* rows);

  ErrorCode UpdateRow(const std::string& table_name,
                      const std::string& branch_name,
                      size_t row_idx, const Row& row);

  ErrorCode UpdateRow(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& ref_col_name,
                      const std::string& ref_val, const Row& row,
                      size_t* n_rows_affected = nullptr);

  ErrorCode UpdateConsecutiveRows(const std::string& table_name,
                                  const std::string& branch_name,
                                  const std::string& ref_col_name,
                                  const std::string& ref_val,
                                  const FuncRow& func_row,
                                  size_t* n_rows_affected = nullptr);

  ErrorCode InsertRow(const std::string& table_name,
                      const std::string& branch_name, const Row& row);

  ErrorCode InsertRowDistinct(const std::string& table_name,
                              const std::string& branch_name,
                              const std::string& dist_col_name,
                              const Row& row);

  ErrorCode DeleteRow(const std::string& table_name,
                      const std::string& branch_name, size_t row_idx);

  ErrorCode DeleteRow(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& ref_col_name,
                      const std::string& ref_val,
                      size_t* n_rows_deleted = nullptr);

  inline ColumnDiffIterator DiffColumn(const Column& lhs, const Column& rhs) {
    return UList::DuallyDiff(lhs, rhs);
  }

  template<class T1, class T2>
  static inline std::string GlobalKey(const T1& table_name,
                                      const T2& col_name) {
    return Utils::ToString(table_name) + "::" + Utils::ToString(col_name);
  }

  ErrorCode GetStorageBytes(size_t* n_bytes);
  ErrorCode GetStorageChunks(size_t* n_chunks);

 private:
  ErrorCode ReadTable(const Slice& table, const Slice& branch, Table* tab);

  ErrorCode ReadColumn(const Slice& col_key, const Hash& col_ver, Column* col);

  ErrorCode WriteColumn(
    const std::string& table_name, const std::string& branch_name,
    const std::string& col_name, const std::vector<std::string>& col_vals,
    Hash* ver);

  ErrorCode LoadCSV(
    std::ifstream& ifs, const std::string& table_name,
    const std::string& branch_name, const std::vector<std::string>& col_names,
    char delim, size_t batch_size, const std::atomic_size_t& total_rows,
    bool print_progress);

  void ShardCSV(
    std::ifstream& ifs, size_t batch_size, size_t n_cols, char delim,
    BlockingQueue<std::vector<std::vector<std::string>>>& batch_queue,
    const ErrorCode& stat_flush);

  void FlushCSV(
    const std::string& table_name, const std::string& branch_name,
    const std::vector<std::string>& col_names,
    BlockingQueue<std::vector<std::vector<std::string>>>& batch_queue,
    const std::atomic_size_t& total_rows, ErrorCode& stat,
    bool print_progress);

  ErrorCode Validate(const Row& row, const std::string& table_name,
                     const std::string& branch_name,
                     size_t* n_fields_not_covered);

  ErrorCode ManipRow(
    const std::string& table_name, const std::string& branch_name,
    size_t row_idx, const Row& row,
    const std::function<void(Column*, const std::string&)> f_manip_col);

  ErrorCode ManipRows(
    const std::string& table_name, const std::string& branch_name,
    const std::string& ref_col_name, const std::string& ref_val,
    const Row& row,
    const std::function<void(
      Column*, size_t row_idx, const std::string&)> f_manip_col,
    size_t* n_rows_affected);

  ErrorCode InsertRow(const Slice& table, const Slice& branch, const Row& row);

  ObjectDB odb_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_RELATIONAL_H_
