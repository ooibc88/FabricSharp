// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_LOGGING_H_
#define USTORE_UTILS_LOGGING_H_

#include <stdlib.h>
#include <sstream>
#include <string>
#include <utility>

#include "type_traits.h"

namespace ustore {

/// Global functions for both glog and built-in log
void InitLogging(const char *argv);
/// Make it so that all log messages go only to stderr
void LogToStderr();
/// Make it so that all log messages of at least a particular severity are
/// logged to stderr (in addtion to logging to the usual log files)
void SetStderrLogging(int severity);
/// Set the file name for logging (and disable logging to stderr)
void SetLogDestination(int severity, const char* path);

using std::string;

const int INFO = 0;            // base_logging::INFO;
const int WARNING = 1;         // base_logging::WARNING;
const int ERROR = 2;           // base_logging::ERROR;
const int FATAL = 3;           // base_logging::FATAL;
const int NUM_SEVERITIES = 4;  // base_logging::NUM_SEVERITIES;

namespace logging {

class LogMessage : public std::basic_ostringstream<char> {
 public:
  LogMessage(const char* fname, int line, int severity);
  ~LogMessage();

 protected:
  void GenerateLogMessage();
  void DoLogging(FILE* file, const struct tm& tm_time);

 private:
  const char* fname_;
  int line_;
  int severity_;
};

// LogMessageFatal ensures the process will exit in failure after
// logging this message.
class LogMessageFatal : public LogMessage {
 public:
  LogMessageFatal(const char* file, int line);
  ~LogMessageFatal();
};

#define _USTORE_LOG_INFO \
  ::ustore::logging::LogMessage(__FILE__, __LINE__, ustore::INFO)
#define _USTORE_LOG_WARNING \
  ::ustore::logging::LogMessage(__FILE__, __LINE__, ustore::WARNING)
#define _USTORE_LOG_ERROR \
  ::ustore::logging::LogMessage(__FILE__, __LINE__, ustore::ERROR)
#define _USTORE_LOG_FATAL \
  ::ustore::logging::LogMessageFatal(__FILE__, __LINE__)

#define LOG(severity) _USTORE_LOG_##severity

/// CHECK dies with a fatal error if condition is not true.  It is *not*
/// controlled by NDEBUG, so the check will be executed regardless of
/// compilation mode.  Therefore, it is safe to do things like:
///    CHECK(fp->Write(x) == 4)
#define CHECK(condition)              \
  if (!(condition)) \
  LOG(FATAL) << "Check failed: " #condition " "

// Function is overloaded for integral types to allow static const
// integrals declared in classes and not defined to be used as arguments to
// CHECK* macros. It's not encouraged though.

// SFINAE to differentiate between integeral and non-integral parameters
template <typename T,
         typename = ::ustore::not_integral_t<
            ::ustore::remove_cv_t<::ustore::remove_reference_t<T>>>>
inline const T& GetReferenceableValue(const T& t) {
    return t;
}

template <typename T, typename = ::ustore::is_integral_t<T>>
inline T GetReferenceableValue(T t) {return t;}

// This formats a value for a failing CHECK_XX statement.  Ordinarily,
// it uses the definition for operator<<, with a few special cases below.
template <typename T, typename = ::ustore::not_char_t<T>>
inline void MakeCheckOpValueString(std::ostream* os, const T& v) {
  (*os) << v;
}

// Overrides for char types provide readable values for unprintable
// characters.
template <typename T, typename = ::ustore::is_char_t<T>>
inline void MakeCheckOpValueString(std::ostream* os, T v) {
  if (v >= 32 && v <= 126) {
    (*os) << "'" << v << "'";
  } else {
    (*os) << "char value " << (short)v;
  }
}

// We need an explicit specialization for std::nullptr_t.
template <>
void MakeCheckOpValueString(std::ostream* os, const std::nullptr_t& p);

// A container for a string pointer which can be evaluated to a bool -
// true iff the pointer is non-NULL.
struct CheckOpString {
  CheckOpString(string* str) : str_(str) {}
  // No destructor: if str_ is non-NULL, we're about to LOG(FATAL),
  // so there's no point in cleaning up str_.
  operator bool() const { return str_ != NULL; }
  string* str_;
};

// Build the error message string. Specify no inlining for code size.
template <typename T1, typename T2>
string* MakeCheckOpString(const T1& v1, const T2& v2,
    const char* exprtext);

// A helper class for formatting "expr (V1 vs. V2)" in a CHECK_XX
// statement.  See MakeCheckOpString for sample usage.  Other
// approaches were considered: use of a template method (e.g.,
// base::BuildCheckOpString(exprtext, base::Print<T1>, &v1,
// base::Print<T2>, &v2), however this approach has complications
// related to volatile arguments and function-pointer arguments).
class CheckOpMessageBuilder {
 public:
  // Inserts "exprtext" and " (" to the stream.
  explicit CheckOpMessageBuilder(const char* exprtext);
  // Deletes "stream_".
  ~CheckOpMessageBuilder();
  // For inserting the first variable.
  std::ostream* ForVar1() { return stream_; }
  // For inserting the second variable (adds an intermediate " vs. ").
  std::ostream* ForVar2();
  // Get the result (inserts the closing ")").
  string* NewString();

 private:
  std::ostringstream* stream_;
};

template <typename T1, typename T2>
string* MakeCheckOpString(const T1& v1, const T2& v2, const char* exprtext) {
  CheckOpMessageBuilder comb(exprtext);
  MakeCheckOpValueString(comb.ForVar1(), v1);
  MakeCheckOpValueString(comb.ForVar2(), v2);
  return comb.NewString();
}

// Helper functions for CHECK_OP macro.
// The (int, int) specialization works around the issue that the compiler
// will not instantiate the template version of the function on values of
// unnamed enum type - see comment below.
#define USTORE_DEFINE_CHECK_OP_IMPL(name, op)                         \
  template <typename T1, typename T2>                                \
  inline string* name##Impl(const T1& v1, const T2& v2,              \
                            const char* exprtext) {                  \
    if (v1 op v2)                                                    \
      return NULL;                                                   \
    else                                                             \
      return ::ustore::logging::MakeCheckOpString(v1, v2, exprtext); \
  }                                                                  \
  inline string* name##Impl(int v1, int v2, const char* exprtext) {  \
    return name##Impl<int, int>(v1, v2, exprtext);                   \
  }

// We use the full name Check_EQ, Check_NE, etc. in case the file including
// base/logging.h provides its own #defines for the simpler names EQ, NE, etc.
// This happens if, for example, those are used as token names in a
// yacc grammar.
USTORE_DEFINE_CHECK_OP_IMPL(Check_EQ,
                           == )  // Compilation error with CHECK_EQ(NULL, x)?
USTORE_DEFINE_CHECK_OP_IMPL(Check_NE, != )  // Use CHECK(x == NULL) instead.
USTORE_DEFINE_CHECK_OP_IMPL(Check_LE, <= )
USTORE_DEFINE_CHECK_OP_IMPL(Check_LT, < )
USTORE_DEFINE_CHECK_OP_IMPL(Check_GE, >= )
USTORE_DEFINE_CHECK_OP_IMPL(Check_GT, > )
#undef USTORE_DEFINE_CHECK_OP_IMPL

// In optimized mode, use CheckOpString to hint to compiler that
// the while condition is unlikely.
#define CHECK_OP_LOG(name, op, val1, val2)                      \
  while (::ustore::logging::CheckOpString _result =              \
             ::ustore::logging::name##Impl(                      \
                 ::ustore::logging::GetReferenceableValue(val1), \
                 ::ustore::logging::GetReferenceableValue(val2), \
                 #val1 " " #op " " #val2))                      \
  ::ustore::logging::LogMessageFatal(__FILE__, __LINE__) << *(_result.str_)

#define CHECK_OP(name, op, val1, val2) CHECK_OP_LOG(name, op, val1, val2)

// CHECK_EQ/NE/...
#define CHECK_EQ(val1, val2) CHECK_OP(Check_EQ, ==, val1, val2)
#define CHECK_NE(val1, val2) CHECK_OP(Check_NE, !=, val1, val2)
#define CHECK_LE(val1, val2) CHECK_OP(Check_LE, <=, val1, val2)
#define CHECK_LT(val1, val2) CHECK_OP(Check_LT, <, val1, val2)
#define CHECK_GE(val1, val2) CHECK_OP(Check_GE, >=, val1, val2)
#define CHECK_GT(val1, val2) CHECK_OP(Check_GT, >, val1, val2)
#define CHECK_NOTNULL(val)                            \
  ::ustore::logging::CheckNotNull(__FILE__, __LINE__, \
                                  "'" #val "' Must be non NULL", (val))

#ifdef DEBUG
// DCHECK_EQ/NE/...
#define DCHECK(condition) CHECK(condition)
#define DCHECK_EQ(val1, val2) CHECK_EQ(val1, val2)
#define DCHECK_NE(val1, val2) CHECK_NE(val1, val2)
#define DCHECK_LE(val1, val2) CHECK_LE(val1, val2)
#define DCHECK_LT(val1, val2) CHECK_LT(val1, val2)
#define DCHECK_GE(val1, val2) CHECK_GE(val1, val2)
#define DCHECK_GT(val1, val2) CHECK_GT(val1, val2)

// wangsh: support debug logging
#define DLOG(severity) LOG(severity)

#else

#define DCHECK(condition) \
  while (false && (condition)) LOG(FATAL)

// NDEBUG is defined, so DCHECK_EQ(x, y) and so on do nothing.
// However, we still want the compiler to parse x and y, because
// we don't want to lose potentially useful errors and warnings.
// _DCHECK_NOP is a helper, and should not be used outside of this file.
#define _USTORE_DCHECK_NOP(x, y) \
  while (false && ((void)(x), (void)(y), 0)) LOG(FATAL)

#define DCHECK_EQ(x, y) _USTORE_DCHECK_NOP(x, y)
#define DCHECK_NE(x, y) _USTORE_DCHECK_NOP(x, y)
#define DCHECK_LE(x, y) _USTORE_DCHECK_NOP(x, y)
#define DCHECK_LT(x, y) _USTORE_DCHECK_NOP(x, y)
#define DCHECK_GE(x, y) _USTORE_DCHECK_NOP(x, y)
#define DCHECK_GT(x, y) _USTORE_DCHECK_NOP(x, y)

// wangsh: support debug logging
#define DLOG(severity) while (false) LOG(severity)

#endif

// These are for when you don't want a CHECK failure to print a verbose
// stack trace.  The implementation of CHECK* in this file already doesn't.
#define QCHECK(condition) CHECK(condition)
#define QCHECK_EQ(x, y) CHECK_EQ(x, y)
#define QCHECK_NE(x, y) CHECK_NE(x, y)
#define QCHECK_LE(x, y) CHECK_LE(x, y)
#define QCHECK_LT(x, y) CHECK_LT(x, y)
#define QCHECK_GE(x, y) CHECK_GE(x, y)
#define QCHECK_GT(x, y) CHECK_GT(x, y)

template <typename T>
T&& CheckNotNull(const char* file, int line, const char* exprtext, T&& t) {
  if (t == nullptr) {
    LogMessageFatal(file, line) << string(exprtext);
  }
  return std::forward<T>(t);
}

}  // namespace logging
}  // namespace ustore

#endif  // USTORE_UTILS_LOGGING_H_
