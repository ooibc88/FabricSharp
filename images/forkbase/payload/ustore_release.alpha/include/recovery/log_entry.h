// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_LOG_ENTRY_H_
#define USTORE_RECOVERY_LOG_ENTRY_H_

#include <string>
#include "recovery/record_header.h"

namespace ustore {
namespace recovery {

enum LogCommand : uint8_t {
  kCheckpoint = 101;  // write checkpoints
  // TODO(yaochang): add log command when all logics are done
};

/*
 * @brief Introduction to LogEntry
 * Log Entry for UStore
 * one Log record consists of four parts:
 *     RecordHeader + Sequence ID + Log Command ID + Log content
 * */
class LogEntry {
 public:
  static constexpr uint16_t kLogVersion = 1;

  LogEntry() {
    memset(this, 0x00, sizeof(LogEntry));
  }
  ~LogEntry() {}

  inline void setSeqId(uint64_t seq_id) { seq_id_ = seq_id; }
  inline void setCmdId(uint32_t  cmd_id) { cmd_id_ = cmd_id; }
  std::string ToString() const;
  int64_t ToString(char* buf, uint64_t len) const;
  /*
   * @brief Set record header in the Log Entry
   * @param log buffer address
   * @param content length
   * */
  int FillRecordHeader(const char* log_data, const data_len);
  /*
   * @brief Calculating the checksum of the log entry:
   *            sequence_id + cmd_id + log content
   * @param log buffer address
   * @param content length
   * */
  int64_t ComputeChecksum(const char* log_data, uint64_t data_len) const;
  /*
   * @brief Return the length of the log data
   * */
  uint32_t GetLogdataLength() const;
  /*
   * @brief Check the structure correctnesss of the record header
   * */
  bool CheckRecordHeader() const;
  /*
   * @brief Check the correctness of the log data
   * */
  bool CheckLogdata() const;

 private:
  RecordHeader header_;
  uint64_t seq_id_;
  uint32_t cmd_id_;
};  // LogEntry

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_ENTRY_H_
