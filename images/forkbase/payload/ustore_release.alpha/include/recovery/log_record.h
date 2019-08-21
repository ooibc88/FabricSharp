// Copyright (c) 2017 The Ustore Authors

#ifndef USTORE_RECOVERY_LOG_RECORD_H_
#define USTORE_RECOVERY_LOG_RECORD_H_

#include <memory>
#include <string>
#include "types/type.h"

namespace ustore {
namespace recovery {

enum class LogCommand : int16_t {
  kNull = 0,  // Invalid
  kUpdate = 111,  // Update(branch_name, version)
  kRename = 112,  // Rename(branch_name, new_branch_name)
  kRemove = 113   // Remove(branch_name)
};
  /*
   * the structure of one log record is:
   * total_length||checksum||version||logcmd||lsn||key_len||value_len||key||value
   **/
struct LogRecord {
 public:
  LogRecord() {}
  ~LogRecord() = default;

  // generate all the content to a string
  std::string ToString();
  // generate all the content to a string
  void FromString(const char* log_data);
  // Compute the checksum according to the content
  int64_t ComputeChecksum();

  // compute the checksum after the other fields are filled
  int64_t checksum = 0;
  int16_t version = 1;  // by default
  LogCommand logcmd = LogCommand::kNull;  // what kind of operation
  int64_t log_sequence_number = 0;
  const byte_t* key = nullptr;
  const byte_t* value = nullptr;
  int64_t key_length = 0;
  int64_t value_length = 0;
  int64_t data_length = 0;
  std::unique_ptr<byte_t[]> key_data;
  std::unique_ptr<byte_t[]> value_data;
};

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_RECORD_H_
