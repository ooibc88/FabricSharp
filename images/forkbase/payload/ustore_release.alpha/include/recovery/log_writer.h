// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_LOG_WRITER_H_
#define USTORE_RECOVERY_LOG_WRITER_H_

#include "recovery/log_cursor.h"

namespace ustore {
namespace recovery {


/*
 * Log Writer class, can be inherited by others
 * */
class LogWriter {
 public:
  LogWriter();
  virtual ~LogWriter();

  int Init(const char* log_dir, int64_t align_mask, int64_t log_sync_type);
  int Rreset();
  /*
   * check the log cursor and and do all the checking works before writing
   * the logs out
   * */
  int StartLog(const LogCursor* log_cursor);
  /*
   * Before write the log out, it needs to check the states, do parsing, etc.
   * */
  int WriteLog(const char* log_data, uint64_t data_length);
  /*
   * @params [out] log_cursor
   * */
  int GetLogCursor(LogCursor* log_cursor) const;
  int FlushLogToDisk();

 protected:
  char* log_dir_;  // log directory
  /*
   * Synchronization type:
   * 0 => default value, log writer flush the logs out when the buffer is full
   *      or the time is out.
   * 1 => before each operation to finish, log writer flush the log records out
   *      to provide strong consistency
   * */
  int64_t log_sync_type_;
  int64_t log_sync_time;  // ms
  LogCursor log_cursor_;
  FileAppender file_;
};

/*
 * @brief open FileAppender for a Writer class
 * @params [out] file
 * @params [in]  log_dir
 * @params [in]  log_file_id
 * @params [in]  is truncation or not
 * */
static int openLogFile(const char* log_dir, uint64_t log_file_id, bool is_trunc,
                       FileAppender* file);

/*
 * if the log file is out of space, then a new file are needed
 * @params [out] file
 * @params [in]  log_dir
 * @params [in]  old_log_file_id
 * @params [in]  new_log_file_id
 * @params [in]  is truncation or not
 * */
static int openNewLogFile(const char* log_dir, uint64_t old_log_file_id,
                          uint64_t new_log_file_id, bool is_trunc,
                          FileAppender* file);

/*
 * parse the log data in the buffer
 * @params [in] log_data
 * @params [in] data_length
 * @params [in] start log cursor
 * @params [out] end log cursor
 * @params [in] check integrity or not
 * */
static int parseLogBuffer(const char* log_data, uint64_t data_length,
                          const LogCursor&  start_cursor, LogCursor* end_cursor,
                          bool check_data_integrity = false);

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_WRITER_H_
