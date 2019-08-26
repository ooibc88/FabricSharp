// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_LOCK_H_
#define USTORE_HTTP_LOCK_H_

#include "http/settings.h"

namespace ustore {
#if !defined(USE_ATOMIC)
#include <mutex>
#endif

/*
 * a lock wrapper which can be changed to mutex-based or atomic-based easily
 */
class Locker {
#ifdef USE_ATOMIC
  bool lock_ = 0;
#else
  std::mutex lock_;
#endif

 public:
  inline void lock() {
#ifdef USE_ATOMIC
    while (__atomic_test_and_set(&lock_, __ATOMIC_RELAXED)) {}
#else
    lock_.lock();
#endif
  }

  inline void unlock() {
#ifdef USE_ATOMIC
    __atomic_clear(&lock_, __ATOMIC_RELAXED);
#else
    lock_.unlock();
#endif
  }

  inline bool try_lock() {
#ifdef USE_ATOMIC
    return !__atomic_test_and_set(&lock_, __ATOMIC_RELAXED);
#else
    return lock_.try_lock();
#endif
  }
};
}  // namespace ustore

#endif  // USTORE_HTTP_LOCK_H_
