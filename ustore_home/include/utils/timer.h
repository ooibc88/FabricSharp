// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_UTILS_TIMER_H_
#define USTORE_UTILS_TIMER_H_

#include <chrono>
#include <iostream>
#include <ratio>
#include <string>
#include <unordered_map>
#include "utils/logging.h"
#include "utils/noncopyable.h"
#include "utils/singleton.h"

namespace ustore {

using namespace std::chrono;

class Timer : private Noncopyable {
 public:
  Timer() { Reset(); }
  ~Timer() = default;

  inline Timer& Reset() {
    elapsed_ = {};
    counter_ = 0;
    running_ = false;
    return *this;
  }

  inline Timer& Start() {
    CHECK(!running_) << "Timer is already started";
    running_ = true;
    ++counter_;
    start_ = clock::now();
    return *this;
  }

  inline Timer& Stop() {
    CHECK(running_) << "Timer is already stopped";
    elapsed_ += clock::now() - start_;
    running_ = false;
    return *this;
  }

  inline size_t GetStartCounter() { return counter_; }

  inline double ElapsedNanoseconds() const { return Elapsed<std::nano>(); }
  inline double ElapsedMicroseconds() const { return Elapsed<std::micro>(); }
  inline double ElapsedMilliseconds() const { return Elapsed<std::milli>(); }
  inline double ElapsedSeconds() const { return Elapsed<seconds::period>(); }
  inline double ElapsedMinutes() const { return Elapsed<minutes::period>(); }
  inline double ElapsedHours() const { return Elapsed<hours::period>(); }
  inline double ElapsedDays() const { return Elapsed<std::ratio<86400>>(); }

  static inline double TimeNanoseconds(const std::function<void()>& f) {
    return Time<std::nano>(f);
  }

  static inline double TimeMicroseconds(const std::function<void()>& f) {
    return Time<std::micro>(f);
  }

  static inline double TimeMilliseconds(const std::function<void()>& f) {
    return Time<std::milli>(f);
  }

  static inline double TimeSeconds(const std::function<void()>& f) {
    return Time<seconds::period>(f);
  }

  static inline double TimeMinutes(const std::function<void()>& f) {
    return Time<minutes::period>(f);
  }

  static inline double TimeHours(const std::function<void()>& f) {
    return Time<hours::period>(f);
  }

  static inline double TimeDays(const std::function<void()>& f) {
    return Time<std::ratio<86400>>(f);
  }

 private:
  using clock = high_resolution_clock;

  template<typename T>
  static inline double Time(const std::function<void()>& func) {
    clock::time_point start(clock::now());
    func();
    return ToUnit<T>(clock::now() - start);
  }

  template<typename T>
  static inline double ToUnit(const clock::duration& time_span) {
    return duration_cast<duration<double, T>>(time_span).count();
  }

  template<typename T>
  inline double Elapsed() const {
    return ToUnit<T>(running_ ? elapsed_ + (clock::now() - start_) : elapsed_);
  }

  clock::duration elapsed_;
  clock::time_point start_;
  size_t counter_;
  bool running_;
};

class TimerPool : private Noncopyable, public Singleton<TimerPool> {
  friend class Singleton<TimerPool>;
 public:
  static Timer& GetTimer(const std::string& name) {
    return TimerPool::Instance()->timers_[name];
  }

  static size_t ListTimers() {
    auto& timers = TimerPool::Instance()->timers_;
    for (auto& kv : timers) {
      auto& tm = kv.second;
      LOG(INFO) << "[Timer] Tag: \"" << kv.first << "\", Elapsed: "
                << tm.ElapsedMilliseconds() << " ms, Counter: "
                << tm.GetStartCounter() << " ops";
    }
    return timers.size();
  }

 private:
  TimerPool() = default;
  ~TimerPool() { ListTimers(); }

  std::unordered_map<std::string, Timer> timers_;
};

}  // namespace ustore

#endif  // USTORE_UTILS_TIMER_H_
