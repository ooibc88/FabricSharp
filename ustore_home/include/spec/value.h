// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_VALUE_H_
#define USTORE_SPEC_VALUE_H_

#include <iostream>
#include <memory>
#include <vector>
#include "hash/hash.h"
#include "types/type.h"
#include "spec/slice.h"
#include "utils/logging.h"

namespace ustore {

// This struct is generalized to inserts/updates any UTypes
struct Value {
  // Type of the value
  UType type;
  // Hash::kNull if it is a new insertion
  // Otherwise, it is an update from the content that has the Hash
  Hash base;
  // Only used for an update
  // Indicate where to start the insertion/deletion
  size_t pos;
  // Number of deletions
  size_t dels;
  // Content of Insertions
  // size = 1 for Blob/String
  // size > 1 for Map/List
  std::vector<Slice> vals;
  // Only used my Map, and has keys.size() = vals.size()
  std::vector<Slice> keys;

  // Application specific context
  Slice ctx;
};

}  // namespace ustore

#endif  // USTORE_SPEC_VALUE_H_
