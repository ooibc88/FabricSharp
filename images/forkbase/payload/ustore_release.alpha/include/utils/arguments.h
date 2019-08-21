// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_ARGUMENTS_H_
#define USTORE_UTILS_ARGUMENTS_H_

#include <boost/program_options.hpp>
#include <iostream>
#include <string>
#include <vector>
#include "utils/logging.h"
#include "utils/utils.h"

namespace ustore {

namespace po = boost::program_options;

class Arguments {
 public:
  bool is_help = false;

  Arguments() noexcept {
    Add(&is_help, "help", "?", "print usage message");
  }

  ~Arguments() = default;

  bool ParseCmdArgs(int argc, char* argv[]);

  bool ParseCmdArgs(const std::vector<std::string>& args);

 protected:
  virtual bool CheckArgs() { return true; }
  virtual std::string MoreHelpMessage() { return ""; }

  template<typename T>
  static bool Check(const T& var, const bool expr, const std::string& title,
                    const std::string& expect);

  template<typename T1, typename T2>
  static inline bool CheckEQ(const T1& var, const T2& expect,
                             const std::string& title) {
    return Check(var, var == expect, title, "=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckNE(const T1& var, const T2& expect,
                             const std::string& title) {
    return Check(var, var != expect, title, "!=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckLE(const T1& var, const T2& expect,
                             const std::string& title) {
    return Check(var, var <= expect, title, "<=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckLT(const T1& var, const T2& expect,
                             const std::string& title) {
    return Check(var, var < expect, title, "<" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckGE(const T1& var, const T2& expect,
                             const std::string& title) {
    return Check(var, var >= expect, title, ">=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckGT(const T1& var, const T2& expect,
                             const std::string& title) {
    return Check(var, var > expect, title, ">" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound <= var && var <= ubound, title, "range of " +
                 Utils::ToStringPair(lbound, ubound, "[", "]", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckInLeftOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound < var && var <= ubound, title, "range of " +
                 Utils::ToStringPair(lbound, ubound, "(", "]", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckInRightOpenInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound <= var && var < ubound, title, "range of " +
                 Utils::ToStringPair(lbound, ubound, "[", ")", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckInOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound < var && var < ubound, title, "range of " +
                 Utils::ToStringPair(lbound, ubound, "(", ")", ","));
  }

  // for string argument
  inline void Add(std::string* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const std::string& deft_val = "") {
    args_.emplace_back(
      Meta<std::string>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void AddHidden(std::string* param_ptr, const std::string& name_long,
                        const std::string& deft_val = "") {
    hidden_args_.emplace_back(
      Meta<std::string>({param_ptr, name_long, "", "", deft_val}));
  }
  inline void AddPositional(std::string* param_ptr,
                            const std::string& name_long,
                            const std::string& desc,
                            const std::string& deft_val = "") {
    auto desc_ext = AddPositional(name_long, desc);
    Add(param_ptr, name_long, "", desc_ext, deft_val);
  }

  // for bool argument
  inline void Add(bool* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc) {
    bool_args_.emplace_back(
      Meta<bool>({param_ptr, name_long, name_short, desc, false}));
  }
  inline void AddHidden(bool* param_ptr, const std::string& name_long) {
    hidden_bool_args_.emplace_back(
      Meta<bool>({param_ptr, name_long, "", "", false}));
  }

  // for int argument
  inline void Add(int* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const int& deft_val = 0) {
    int_args_.emplace_back(
      Meta<int>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void AddHidden(int* param_ptr, const std::string& name_long,
                        const int& deft_val = 0) {
    hidden_int_args_.emplace_back(
      Meta<int>({param_ptr, name_long, "", "", deft_val}));
  }
  inline void AddPositional(int* param_ptr, const std::string& name_long,
                            const std::string& desc, const int& deft_val = 0) {
    auto desc_ext = AddPositional(name_long, desc);
    Add(param_ptr, name_long, "", desc_ext, deft_val);
  }

  // for int64_t argument
  inline void Add(int64_t* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const int64_t& deft_val = 0) {
    int64_args_.emplace_back(
      Meta<int64_t>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void AddHidden(int64_t* param_ptr, const std::string& name_long,
                        const int64_t& deft_val = 0) {
    hidden_int64_args_.emplace_back(
      Meta<int64_t>({param_ptr, name_long, "", "", deft_val}));
  }
  inline void AddPositional(int64_t* param_ptr, const std::string& name_long,
                            const std::string& desc,
                            const int64_t& deft_val = 0) {
    auto desc_ext = AddPositional(name_long, desc);
    Add(param_ptr, name_long, "", desc_ext, deft_val);
  }

  // for double argument
  inline void Add(double* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const double& deft_val = 0.0) {
    double_args_.emplace_back(
      Meta<double>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void AddHidden(double* param_ptr, const std::string& name_long,
                        const double& deft_val = 0.0) {
    hidden_double_args_.emplace_back(
      Meta<double>({param_ptr, name_long, "", "", deft_val}));
  }
  inline void AddPositional(double* param_ptr, const std::string& name_long,
                            const std::string& desc,
                            const double& deft_val = 0.0) {
    auto desc_ext = AddPositional(name_long, desc);
    Add(param_ptr, name_long, "", desc_ext, deft_val);
  }

 private:
  template<typename T>
  struct Meta {
    T* param_ptr;
    const std::string name_long;
    const std::string name_short;
    const std::string desc;
    const T deft_val;
  };

  std::vector<Meta<std::string>> args_;
  std::vector<Meta<bool>> bool_args_;
  std::vector<Meta<int>> int_args_;
  std::vector<Meta<int64_t>> int64_args_;
  std::vector<Meta<double>> double_args_;

  std::vector<Meta<std::string>> hidden_args_;
  std::vector<Meta<bool>> hidden_bool_args_;
  std::vector<Meta<int>> hidden_int_args_;
  std::vector<Meta<int64_t>> hidden_int64_args_;
  std::vector<Meta<double>> hidden_double_args_;

  std::vector<std::string> pos_arg_names_;

  inline std::string AddPositional(const std::string& name_long,
                                   const std::string& desc) {
    pos_arg_names_.emplace_back(name_long);
    return desc + " [Positional: " +
           Utils::ToString(pos_arg_names_.size()) + "]";
  }

  inline void AssignArgs(const std::vector<Meta<std::string>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<std::string>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<bool>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm.count(meta.name_long) ? true : false;
    }
  }
  inline void AssignArgs(const std::vector<Meta<int>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<int>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<int64_t>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<int64_t>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<double>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<double>();
    }
  }

  template<typename T>
  void AddArgs(const std::vector<Meta<T>>& args, po::options_description* od);

  void AddBoolArgs(const std::vector<Meta<bool>>& args,
                   po::options_description* od);

  bool ParseCmdArgs(int argc, char* argv[], po::variables_map* vm);
};

template<typename T>
bool Arguments::Check(const T& var, const bool expr,
                      const std::string& title, const std::string& expect) {
  if (expr) {
    LOG(INFO) << "[ARG] " << title << ": " << var;
  } else {
    std::cerr << BOLD_RED("[ERROR ARG] ") << title << ": "
              << "[Actual] " << var << ", [Expected] " << expect << std::endl;
  }
  return expr;
}

template<typename T>
void Arguments::AddArgs(const std::vector<Meta<T>>& args,
                        po::options_description* od) {
  for (auto& meta : args) {
    auto& name_long = meta.name_long;
    auto& name_short = meta.name_short;
    auto cfg = name_long + (name_short.empty() ? "" : "," + name_short);
    auto& desc = meta.desc;
    auto& deft_val = meta.deft_val;

    od->add_options()
    (cfg.c_str(), po::value<T>()->default_value(deft_val), desc.c_str());
  }
}

}  // namespace ustore

#endif  // USTORE_UTILS_ARGUMENTS_H_
