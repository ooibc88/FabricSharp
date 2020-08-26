// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SSET_H_
#define USTORE_TYPES_SERVER_SSET_H_

#include <memory>
#include <vector>

#include "chunk/chunk_writer.h"
#include "types/uset.h"

namespace ustore {

class SSet : public USet {
  friend class ChunkableTypeFactory;
 public:
  SSet(SSet&&) = default;
  SSet& operator=(SSet&&) = default;
  ~SSet() = default;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  Hash Set(const Slice& key) const override;
  Hash Remove(const Slice& key) const override;

  // Use this Set as base to perform three-way merging
  //   return empty hash when merging fails
  Hash Merge(const SSet& node1, const SSet& node2) const;

 protected:
  // Load existing SSet
  SSet(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
       const Hash& root_hash) noexcept;
  // Create new SSet
  // kv_items must be sorted in strict ascending order based on key
  SSet(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
       const std::vector<Slice>& keys) noexcept;

 private:
  ChunkWriter* chunk_writer_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_SERVER_SSET_H_
