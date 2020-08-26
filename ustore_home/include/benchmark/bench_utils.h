// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCH_UTILS_H_
#define USTORE_BENCHMARK_BENCH_UTILS_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "spec/slice.h"

namespace ustore {

static const char alphabet[] =
  "0123456789"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz";

class RandomGenerator {
 public:
  RandomGenerator();
  ~RandomGenerator() = default;

  std::string FixedString(int length);
  std::vector<std::string> NFixedString(int size, int length);
  std::string RandomString(int maxLength);
  std::vector<std::string> NRandomString(int size, int maxLength);
  std::vector<std::string> SequentialNumString(const std::string& prefix,
                                               int size);
  std::vector<std::string> PrefixSeqString(const std::string& prefix, int size,
                                           int mod);
  std::vector<std::string> PrefixRandString(const std::string& prefix, int size,
                                           int mod);
  inline int RandomInt(const int min, const int max) {
    std::uniform_int_distribution<> dist(min, max);
    return dist(engine_);
  }

  template<typename T>
  inline void Shuffle(std::vector<T>* elems) {
    std::shuffle(elems->begin(), elems->end(), engine_);
  }

 private:
  std::default_random_engine engine_;
  std::uniform_int_distribution<> alph_dist_;
};

class Profiler {
 public:
  static const unsigned kSamplingInterval;  // milliseconds
  explicit Profiler(size_t n_thread);
  ~Profiler();
  void SamplerThread();
  void Clear();
  double PeakThroughput();
  inline void Terminate() { finished_.store(true, std::memory_order_release); }
  inline void IncCounter(size_t n) {
    counters_[n].fetch_add(1, std::memory_order_acq_rel);
  }

 private:
  size_t n_thread_;
  std::atomic<unsigned>* counters_;
  std::vector<std::vector<unsigned>> samples_;
  std::atomic<bool> finished_;
};

}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCH_UTILS_H_
