// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_DEBUG_H_
#define USTORE_UTILS_DEBUG_H_

#include <string>
#include <vector>

#include "types/type.h"

namespace ustore {

std::string byte2str(const byte_t* data, size_t num_bytes);

// A utility method to splice bytes array, delete @arg num_delete bytes,
// from @arg start, then insert @arg append_size number of bytes from
// input buffer @arg append. This method will not rewrite the original
// @arg src but copy the content of it. Caller of this method is responsible
// for delete of the created buffer.
const ustore::byte_t* SpliceBytes(const ustore::byte_t* src, size_t src_size,
    size_t start, size_t num_delete,
    const ustore::byte_t* append, size_t append_size);

// Perform multiple splice operation on the original content
// idxs is a vector of starting index of original content
// to perform splice on. Must be in strict ascending order.
const ustore::byte_t* MultiSplice(
    const ustore::byte_t* original_content,
    size_t original_num_bytes, std::vector<size_t> idxs,
    std::vector<size_t> num_bytes_remove,
    std::vector<const byte_t*> inserted_data,
    std::vector<size_t> inserted_num_bytes,
    size_t* result_num_bytes);

}  // namespace ustore
#endif  // USTORE_UTILS_DEBUG_H_
