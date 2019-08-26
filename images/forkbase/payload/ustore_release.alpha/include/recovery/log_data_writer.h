// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_LOG_DATA_WRITER_H_
#define USTORE_RECOVERY_LOG_DATA_WRITER_H_

#include "recovery/log_cursor.h"

namespace ustore {
namespace recovery {

/*
 * LogDataAppendBuffer is used in LogDataWriter to buffer writeout log data
 * */
class LogDataAppendBuffer {
 public:
  static const uint64_t DEFAULT_BUFFER_SIZE = 1<<22;
  LogDataAppendBuffer();
  ~LogDataAppendBuffer();
  /*
   * @brief write the log data to the Append buf
   * @param [in] buf: input buffer
   * @param [in] length: input data length
   * @param [in] pos: offset in the append buffer
   * */
  int Write(const char* buf, uint64_t length, uint64_t pos);
  /*
   * @breif: flush the data in the buffer the log file
   * */
  int Flush(int fd);
 private:
  char* buffer_;
  uint64_t buffer_remaining_;
};

class LogDataWriter {
 public:
  LogDataWriter();
  ~LogDataWriter();

  int Init(const char* log_dir, uint64_t file_size, int log_sync_type);
  int Write(const LogCursor* start_cursor, const LogCursor* end_cursor,
            const char* data, uint64_t data_length);
  /*
   * @brief check the log cursors and do the other checking works
   * */
  int StartLog(const LogCursor* log_cursor);
  int Reset();
  int GetLogCursor(LogCursor* log_cursor) const;
  inline int GetFileSize() { return file_size_; }
  uint64_t ToString(char* dest_buf, uint64_t length) const;

 protected:
  int CheckEOF(const LogCursor* log_cursor);
  int PrepareFd(uint64_t file_id);
  int Reuse(const char* pool_file, const char* fname);
  const char* SelectPoolFile(char* fname, uint64_t limit);

 private:
  LogDataAppendBuffer write_buffer_;
  const char* log_dir_;
  uint64_t file_size_;
  LogCursor end_cursor_;
  int log_sync_type_;
  int fd_;
  uint64_t cur_file_id_;
  uint64_t num_file_to_add_;
  uint64_t min_file_id_;
};

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_DATA_WRITER_H_
