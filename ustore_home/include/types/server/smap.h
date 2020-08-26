// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SMAP_H_
#define USTORE_TYPES_SERVER_SMAP_H_

#include <memory>
#include <vector>

#include "chunk/chunk_writer.h"
#include "types/umap.h"

namespace ustore {

class SMap : public UMap {
  friend class ChunkableTypeFactory;
 public:
  SMap(SMap&&) = default;
  SMap& operator=(SMap&&) = default;
  ~SMap() = default;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  Hash Set(const Slice& key, const Slice& val) const override;
  Hash Set(const std::vector<Slice>& keys,
           const std::vector<Slice>& vals) const override;
  Hash Remove(const Slice& key) const override;

  // Use this map as base to perform three-way merging
  //   return empty hash when merging fails
  Hash Merge(const SMap& node1, const SMap& node2) const;

 protected:
  // Load existing SMap
  SMap(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
       const Hash& root_hash) noexcept;
  // Create new SMap
  // kv_items must be sorted in strict ascending order based on key
  SMap(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
       const std::vector<Slice>& keys, const std::vector<Slice>& vals) noexcept;

 private:
  ChunkWriter* chunk_writer_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_SERVER_SMAP_H_
