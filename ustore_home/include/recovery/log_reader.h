// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_LOG_READER_H_
#define USTORE_RECOVERY_LOG_READER_H_

#include "recovery/log_entry.h"
#include "recovery/log_cursor.h"
#include "recovery/single_log_reader.h"

namespace ustore {
namespace recovery {
class LogReader {
 public:
    static const uint64_t WAIT_TIME = 1000000;  // us ==> 1s
    static const uint64_t FAIL_TIME = 60;

 public:
    UStoreLogReader();
    virtual ~UStoreLogReader();
/*
* @brief   initailize UStoreLogReader class, which should be invoked after
* the object malloc and before the other functions' invokation
* @param   [in]    reader: read a single log file
* @param   [in]    log_dir: log directory
* @param   [in]    log_file_id_start: start file id
* @param   [in]    log_seq: previous log entry sequence id that are read
* @param   [in]    is_retry: whether retry when error occurs
* */
    int Init(SingleLogReader *reader, const char* log_dir,
             uint64_t log_file_id_start, uint64_t log_seq, bool is_retry);
/*
*  @brief  read a log entry from the log file. If the log command is
*  USTORE_SWITCH_LOG, the open the next log file directly.
*  However, if next log file does not exist,
*  it may because the log under construction.
*  In this case, wait 1ms and retry 10 times. Otherwise, return error
*  @return USTORE_LOG_SUCCESS, USTORE_LOG_NOTHING, USTORE_LOG_ERROR
* */
    int ReadLog(LogCommand* cmd, uint64_t* seq_id,
                char** log_data, uint64_t* data_length);
    void SetMaxLogFileId(uint64_t max_log_file_id);
    uint64_t GetMaxLogFileId() const;
    bool GetHasMax() const;
    int  SetHasNoMax();
    uint64_t GetCurLogFileId();
    uint64_t GetCurLogSeqId();
    uint64_t GetLastLogOffset();
    int GetCursor(LogCursor* cursor);

 private:
    int Seek_(uint64_t log_seq);
    int OpenLog_(uint64_t log_file_id, uint64_t last_log_seq = 0);
    int ReadLog_(LogCommand* cmd, uint64_t*  log_seq,
                 char** log_data, uint64_t* data_length);
    uint64_t cur_log_file_id_;
    uint64_t cur_log_seq_id_;
    uint64_t max_log_file_id_;
    SingleLogReader log_file_reader_;
    bool is_init_;
    bool is_retry_;
    bool has_max_;
};
}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_READER_H_
