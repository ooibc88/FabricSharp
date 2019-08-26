// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_SYNC_TASK_LINE_H_
#define USTORE_UTILS_SYNC_TASK_LINE_H_

#include <mutex>
#include <utility>
#include "utils/blocking_queue.h"

namespace ustore {

template<class T1, class T2 = int>
class SyncTaskLine {
 public:
  virtual T2 Consume(const T1& data) = 0;
  virtual bool Terminate(const T1& data) = 0;

  SyncTaskLine()
    : queue_(new BlockingQueue<T1>(1)),
      mtx_(new std::mutex()),
      cv_(new std::condition_variable()),
      processed_(false) {}

  ~SyncTaskLine() {
    delete queue_;
    delete mtx_;
    delete cv_;
  }

  inline std::thread Launch() {
    return std::thread([this] { Run(); });
  }

  void Produce(const T1& data) {
    UnsetProcessed();
    queue_->Put(data);
  }

  void Produce(const T1&& data) {
    UnsetProcessed();
    queue_->Put(std::move(data));
  }

  T2 Sync() {
    std::unique_lock<std::mutex> lock(*mtx_);
    cv_->wait(lock, [this] { return processed_; });
    return stat_;
  }

 private:
  inline void UnsetProcessed() {
    std::lock_guard<std::mutex> lock(*mtx_);
    processed_ = false;
  }

  void Run();

  ustore::BlockingQueue<T1>* queue_;
  std::mutex* mtx_;
  std::condition_variable* cv_;
  bool processed_;
  T2 stat_;
};

template<class T1, class T2>
void SyncTaskLine<T1, T2>::Run() {
  T1 data;
  do {
    data = queue_->Take();
    {
      std::lock_guard<std::mutex> lock(*mtx_);
      stat_ = Consume(data);
      processed_ = true;
    }
    cv_->notify_one();
  } while (!Terminate(data));
}

}  // namespace ustore

#endif  // USTORE_UTILS_SYNC_TASK_LINE_H_
