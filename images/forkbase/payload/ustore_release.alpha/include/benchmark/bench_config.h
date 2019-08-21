// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCH_CONFIG_H_
#define USTORE_BENCHMARK_BENCH_CONFIG_H_

#include <boost/program_options.hpp>
#include <iostream>
#include <string>
#include "utils/logging.h"
#include "utils/utils.h"

namespace ustore {

namespace po = boost::program_options;

class BenchmarkConfig {
 public:
  static std::string command;
  static std::string type;
  static bool is_help;
  static int num_clients;
  // common
  static int validate_ops;
  static std::string default_branch;
  static bool suffix;
  static int suffix_range;
  static int ops_amplifier;
  // string
  static int string_ops;
  static int string_length;
  static std::string string_key;
  // blob
  static int blob_ops;
  static int blob_length;
  static std::string blob_key;
  // list
  static int list_ops;
  static int list_length;
  static int list_elements;
  static std::string list_key;
  // map
  static int map_ops;
  static int map_length;
  static int map_elements;
  static std::string map_key;
  // branch
  static int branch_ops;
  static std::string branch_key;
  // merge
  static int merge_ops;
  static std::string merge_key;

  static bool ParseCmdArgs(int argc, char* argv[]);

 private:
  static bool ParseCmdArgs(int argc, char* argv[], po::variables_map* vm);

  template<typename T>
  static bool CheckArg(const T& var, const bool expr,
                       const std::string& title, const std::string& expect);

  template<typename T1, typename T2>
  static inline bool CheckArgEQ(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var == expect, title, "=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgNE(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var != expect, title, "!=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgLE(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var <= expect, title, "<=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgLT(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var < expect, title, "<" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgGE(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var >= expect, title, ">=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgGT(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var > expect, title, ">" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return CheckArg(var, lbound <= var && var <= ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "[", "]", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInLeftOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return CheckArg(var, lbound < var && var <= ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "(", "]", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInRightOpenInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return CheckArg(var, lbound <= var && var < ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "[", ")", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    const std::string expect = "range of (" + Utils::ToString(lbound) +
                               "," + Utils::ToString(ubound) + ")";
    return CheckArg(var, lbound < var && var < ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "(", ")", ","));
  }
};

template<typename T>
bool BenchmarkConfig::CheckArg(const T& var, const bool expr,
                               const std::string& title,
                               const std::string& expect) {
  if (expr) {
    std::cout << "[ARG] " << title << ": " << var << std::endl;
  } else {
    std::cerr << BOLD_RED("[ERROR ARG] ") << title << ": "
              << "[Actual] " << var << ", [Expected] " << expect << std::endl;
  }
  return expr;
}

}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCH_CONFIG_H_
