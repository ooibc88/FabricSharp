%module ustore
%include "std_string.i"

%{
#include "status.h"
%}

namespace ustore_kvdb {

class Status {
 public:
  static Status OK();
  static Status NotFound(const std::string& msg,
                         const std::string& msg2 = std::string());
  static Status Corruption(const std::string& msg,
                           const std::string& msg2 = std::string());
  static Status NotSupported(const std::string& msg,
                             const std::string& msg2 = std::string());
  static Status InvalidArgument(const std::string& msg,
                                const std::string& msg2 = std::string());
  static Status IOError(const std::string& msg,
                        const std::string& msg2 = std::string());
  bool ok() const;
  bool IsNotFound() const;
  bool IsCorruption() const;
  bool IsNotSupported() const;
  bool IsInvalidArgument() const;
  bool IsIOError() const;
  std::string ToString() const;
};

}
