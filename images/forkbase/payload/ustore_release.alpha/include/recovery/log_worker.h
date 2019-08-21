// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_RECOVERY_LOG_WORKER_H_
#define USTORE_RECOVERY_LOG_WORKER_H_

#include <condition_variable>
#include <mutex>

#include "hash/hash.h"
#include"recovery/log_record.h"
#include "recovery/log_thread.h"
#include "spec/slice.h"

namespace ustore {
namespace recovery {

/*
 * Each site should create a LogWorker to write log for the hashtable
 * */
class LogWorker : public LogThread {
 public:
  explicit LogWorker(int stage = 0);
  ~LogWorker();

  bool Init(const char* log_dir, const char* log_filename);
  /*
   * @brief: Update branch version
   * @return: return the log sequence number for the update
   * */
  int64_t Update(const Slice& branch_name, const Hash& new_version);
  /*
   * @brief: Rename the branch name to a new one
   * @return: return the log sequence number for the rename operation
   * */
  int64_t Rename(const Slice& branch_name, const Slice& new_branch_name);
  /*
   * @brief: Remove the branch from the branch head table
   * @return: return the log sequence number for the remove operation
   * */
  int64_t Remove(const Slice& branch_name);
  /*
   * @brief: Read one log record from log file
   * @return: return one log record.
   * */
  bool ReadOneLogRecord(LogRecord* record);
  /*
   * @brief: flush the data in the log buffer to disk and reset related
   * variables
   * */
  int Flush();
  /*
   * @brief: get the defined timeout
   * */
  inline int64_t timeout() { return timeout_; }
  /*
   * @brief: set the timeout according to your application requirement
   * */
  inline void setTimeout(int64_t timeout_ms) { timeout_ = timeout_ms; }
  /*
   * @brief: get the size of the log buffer that is maintained in memory
   * */
  inline int64_t bufferSize() { return buffer_size_; }
  /*
   * @brief: set the size of the log buffer, and should resize the buffer
   * */
  bool setBufferSize(int64_t buf_size);
  /*
   * @brief: get the current position in the log buffer
   * */
  inline int64_t bufferIndice() { return buffer_indice_; }
  /*
   * @brief: get the synchronization type
   * */
  inline int syncType() { return log_sync_type_; }
  /*
   * @brief: set the synchroniation type
   * @params [in] type: 1==>strong consistency, 0==>weak consistency
   * */
  bool setSyncType(int type);

 private:
  static constexpr int64_t kDefaultBufferSize = 4 * 1024 * 1024;
  static constexpr int64_t kDefaultTimeout = 5000;

  int64_t WriteRecord(LogCommand cmd, const Slice& key, const Slice& value);
  /*
   * @brief: get the condition_variable
   * */
  inline std::condition_variable& flushCV() { return flush_cv_; }
  /*
   * @brief: append log data in the log buffer, the log data should contain meta
   * infomation, which is also useful for the recovery.
   * */
  bool WriteLog(const char* data, uint64_t data_length);
  /*
   * @brief: create a thread to do the work
   * */
  void* Run();

  int stage_;  // 0 -> normal execution, 1 -> restart/recovery
  std::condition_variable flush_cv_;  // wait_for timeout or the buffer is full
  std::mutex flush_mutex_;
  int64_t buffer_indice_ = 0;  // current position
  std::mutex buffer_lock_;
  int64_t buffer_size_ = kDefaultBufferSize;
  char* buffer_;  // the log data buffer
  int64_t timeout_ = kDefaultTimeout;
  // const char* log_dir_ = nullptr;
  // const char* log_filename_ = nullptr;
  int fd_ = -1;
  /*
   * log synchronization type may affect the system performance
   * strong consistency ==> 1, each operation can commit only after the log is
   * flushed to disk
   * weak consistency ==> 0, which is also the default setting, the log is
   * flushed to disk when the buffer is full or the timeout is due
   * */
  int log_sync_type_ = 0;
  int64_t log_sequence_number_ = 0;
};

}  // namespace recovery
}  // namespace ustore

#endif   // USTORE_RECOVERY_LOG_WORKER_H_
