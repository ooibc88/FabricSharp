// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_SHARED_LOCK_H_
#define USTORE_UTILS_SHARED_LOCK_H_

#if __cplusplus >= 201402L
#include <shared_mutex>
#else
#include <boost/thread/shared_mutex.hpp>
#endif

namespace ustore {
#if __cplusplus == 201402L  // c++ 14
using shared_mutex = std::shared_timed_mutex;
template <typename Mutex>
using shared_lock = std::shared_lock<Mutex>;
#elif __cplusplus > 201402L  // c++ 17
using shared_mutex = std::shared_mutex;
template <typename Mutex>
using shared_lock = std::shared_lock<Mutex>;
#else  // using boost::shared_mutex
using shared_mutex = boost::shared_mutex;
template <typename Mutex>
using shared_lock = boost::shared_lock<Mutex>;
#endif
}  // namespace ustore

#endif  // USTORE_UTILS_SHARED_LOCK_H_
