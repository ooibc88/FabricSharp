// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_RECOVERY_LOG_THREAD_H_
#define USTORE_RECOVERY_LOG_THREAD_H_

#include <pthread.h>

namespace ustore {
namespace recovery {

/*
 * The base class of LogWriter and LogReader
 * */
class LogThread {
 public:
  LogThread();
  ~LogThread();

  int Start();
  int Join();
  int Detach();
  pthread_t Self();
  virtual void* Run() = 0;  // this function should be implemented!!
 private:
  pthread_t m_tid_;  // the containing thread ID
  int m_running_;  // flag to indicate the thread is running or not: 1 yes, 0 no
  int m_detached_;  // flag the thread is detached or not: 1 yes, 0 no
};

static void* runThread(void* args);

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_THREAD_H_
