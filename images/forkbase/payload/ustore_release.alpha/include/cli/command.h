// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_COMMAND_H_
#define USTORE_CLI_COMMAND_H_

#include <iomanip>
#include <string>
#include <unordered_map>
#include <vector>
#include "spec/blob_store.h"
#include "spec/object_db.h"
#include "spec/relational.h"
#include "utils/timer.h"
#include "utils/utils.h"
#include "cli/config.h"

namespace ustore {
namespace cli {

#define CMD_HANDLER(cmd, handler) do { \
  cmd_exec_[cmd] = [this] { return handler; }; \
} while (0)

#define CMD_ALIAS(cmd, alias) do { \
  alias_exec_[alias] = &cmd_exec_[cmd]; \
} while (0)

#define FORMAT_CMD(cmd, width) \
  "* " << std::left << std::setw(width) << cmd << " "

class Command {
 public:
  explicit Command(DB* db) noexcept;
  ~Command() = default;

  ErrorCode Run(int argc, char* argv[]);

 protected:
  virtual void PrintHelp();
  void PrintCommandHelp(std::ostream& os = std::cout);

  ErrorCode ExecCommand(const std::string& command);

  inline std::string TimeDisplay(const std::string& prefix = "",
                                 const std::string& suffix = "") {
    std::string time_display("");
    if (Config::time_exec && !Config::is_vert_list) {
      time_display =
        prefix + "(in " + Utils::TimeString(time_ms_) + ")" + suffix;
    }
    return time_display;
  }

  inline void Time(const std::function<void()>& f_exec) {
    time_ms_ = Timer::TimeMilliseconds(f_exec);
  }

  std::unordered_map<std::string, std::function<ErrorCode()>> cmd_exec_;
  std::unordered_map<std::string, std::function<ErrorCode()>*> alias_exec_;
  double time_ms_;

 private:
  ErrorCode ExecScript(const std::string& script);

  ErrorCode ExecHelp();
  ErrorCode ExecGet();
  ErrorCode ExecPut();
  ErrorCode ExecMerge();
  ErrorCode ExecBranch();
  ErrorCode ExecRenameBranch();
  ErrorCode ExecDeleteBranch();
  ErrorCode ExecListKey();
  ErrorCode ExecListBranch();
  ErrorCode ExecHead();
  ErrorCode ExecLatest();
  ErrorCode ExecIsHead();
  ErrorCode ExecIsLatest();
  ErrorCode ExecExists();
  ErrorCode ExecCreateTable();
  ErrorCode ExecGetTable();
  ErrorCode ExecBranchTable();
  ErrorCode ExecListTableBranch();
  ErrorCode ExecDeleteTable();
  ErrorCode ExecGetColumn();
  ErrorCode ExecListColumnBranch();
  ErrorCode ExecDeleteColumn();
  ErrorCode ExecDiffTable();
  ErrorCode ExecDiffColumn();
  ErrorCode ExecExistsTable();
  ErrorCode ExecExistsColumn();
  ErrorCode ExecLoadCSV();
  ErrorCode ExecDumpCSV();
  ErrorCode ExecGetRow();
  ErrorCode ExecInsertRow();
  ErrorCode ExecUpdateRow();
  ErrorCode ExecDeleteRow();
  ErrorCode ExecInfo();
  ErrorCode ExecMeta();

  ErrorCode ExecRename();
  ErrorCode ExecDiff();
  ErrorCode ExecAppend();
  ErrorCode ExecUpdate();
  ErrorCode ExecInsert();
  ErrorCode ExecDelete();

  ErrorCode ExecManipMeta(
    const std::function<ErrorCode(const VMeta&)>& f_output_meta);

  ErrorCode PrepareValue(const std::string& cmd);

  ErrorCode ExecPut(const std::string& cmd, const VObject& obj);
  ErrorCode ExecPutString();
  ErrorCode ExecPutBlob();
  ErrorCode ExecPutList();
  ErrorCode ExecPutMap();
  ErrorCode ExecPutSet();
  ErrorCode ExecAppend(VBlob& blob);
  ErrorCode ExecAppend(VList& list);
  ErrorCode ExecUpdate(VBlob& blob);
  ErrorCode ExecUpdate(VList& list);
  ErrorCode ExecUpdate(VMap& map);
  ErrorCode ExecInsert(VBlob& blob);
  ErrorCode ExecInsert(VList& list);
  ErrorCode ExecInsert(VMap& map);
  ErrorCode ExecInsert(VSet& set);
  ErrorCode ExecDelete(VBlob& blob);
  ErrorCode ExecDelete(VList& list);
  ErrorCode ExecDelete(VMap& map);
  ErrorCode ExecDelete(VSet& set);

  ErrorCode ParseRowString(const std::string& row_str, Row* row);

  ErrorCode ValidateDistinct(const std::string& cmd,
                             const std::vector<std::string>& elems,
                             const VList& list,
                             size_t ignored_pos = Utils::max_size_t);

  ErrorCode ExecListDataset();
  ErrorCode ExecCreateDataset();
  ErrorCode ExecExistsDataset();
  ErrorCode ExecDeleteDataset();
  ErrorCode ExecGetDataset();
  ErrorCode ExecExportDatasetBinary();
  ErrorCode ExecBranchDataset();
  ErrorCode ExecListDatasetBranch();
  ErrorCode ExecDiffDataset();
  ErrorCode ExecPutDataEntry();
  ErrorCode ExecPutDataEntryBatch();
  ErrorCode ExecExistsDataEntry();
  ErrorCode ExecGetDataEntry();
  ErrorCode ExecGetDataEntryBatch();
  ErrorCode ExecDeleteDataEntry();
  ErrorCode ExecListDataEntryBranch();

  ErrorCode PrepareDataEntryName(const std::string& cmd);


  ErrorCode ExecGetAll();
  ErrorCode ExecListKeyAll();
  ErrorCode ExecLatestAll();
  ErrorCode ExecGetColumnAll();
  ErrorCode ExecListDatasetAll();
  ErrorCode ExecGetDatasetAll();

  ErrorCode ExecGetStoreSize();

  static const size_t kDefaultLimitPrintElems;
  static size_t limit_print_elems;

  ObjectDB odb_;
  ColumnStore cs_;
  BlobStore bs_;
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_COMMAND_H_
