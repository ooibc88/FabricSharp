// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_SERVICE_H_
#define USTORE_CLUSTER_SERVICE_H_

#include <memory>
#include <thread>
#include "net/net.h"
#include "utils/noncopyable.h"

namespace ustore {

/**
 * Service Run() calls Start() in a background thread
 */
class Service : private Noncopyable {
 public:
  Service() = default;
  virtual ~Service() { Stop(); }

  // Init network before start
  virtual void Init() = 0;
  // Start in current thread (blocking)
  void Start();
  // Run in another thread (non-blocking)
  void Run();
  // Stop service
  void Stop();
  // check status
  inline bool IsRunning() { return is_running_; }

 protected:
  // Need to be called in Init()
  void Init(std::unique_ptr<Net> net, std::unique_ptr<CallBack> callback);

  std::unique_ptr<CallBack> cb_;
  std::unique_ptr<Net> net_;

 private:
  // thread run in background
  std::unique_ptr<std::thread> thread_;
  volatile bool is_running_ = false;
  volatile bool is_init_ = false;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_SERVICE_H_
