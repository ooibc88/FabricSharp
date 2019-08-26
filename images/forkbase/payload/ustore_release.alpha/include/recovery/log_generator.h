// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_LOG_GENERATOR_H_
#define USTORE_RECOVERY_LOG_GENERATOR_H_

#include "recovery/log_cursor.h"
#include "recovery/log_entry.h"

namespace ustore {
namespace recovery {

class LogGenerator {
 public:
  LogGenerator();
  ~LogGenerator();
  int Init(uint64_t log_buf_size, uint64_t log_file_max_size,
           UStoreServer* server = NULL);
  int Reset();
  bool IsLogStart();
  int StartLog(LogCursor* start_cursor);
  int UpdateCursor(LogCursor* log_cursor);
  int WriteLog(LogCommand cmd, const char* log_data, uint64_t data_length);
  template<typename T>
  int WriteLog(LogCommand cmd, T* data);
  int GetLog(LogCursor* start_cursor, LogCursor* end_cursor,
             char** buf, uint64_t* length);
  int Commit(LogCursor* end_cursor);
  int SwitchLog(uint64_t* new_file_id);
  int Checkpoint(uint64_t* cur_log_file_id);
  uint64_t ToString(char* buf, uint64_t length) const;
};

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_GENERATOR_H_
