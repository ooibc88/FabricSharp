// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_UTILS_MESSAGE_PARSER_H_
#define USTORE_UTILS_MESSAGE_PARSER_H_

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "proto/messages.pb.h"
#include "utils/logging.h"

namespace ustore {

class MessageParser {
 public:
  // TODO(wangsh): handle large messages in a robust way
  static bool Parse(const void* msg, int size, UMessage* umsg) {
    // parse small message (< 60M)
    if (size < (60 << 20)) {
      return umsg->ParseFromArray(msg, size);
    }
    // parse large message (< 2GB)
    ::google::protobuf::io::ArrayInputStream buf(msg, size);
    ::google::protobuf::io::CodedInputStream decoder(&buf);
    decoder.SetTotalBytesLimit((1L << 31) - 1, 1L << 30);
    bool success = umsg->ParseFromCodedStream(&decoder)
                   && decoder.ConsumedEntireMessage();
    if (!success) LOG(ERROR) << "Fail to consume entire message";
    return success;
  }
};

}  // namespace ustore

#endif  // USTORE_UTILS_MESSAGE_PARSER_H_
