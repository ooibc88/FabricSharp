// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SLIST_H_
#define USTORE_TYPES_SERVER_SLIST_H_

#include <memory>
#include <vector>

#include "chunk/chunk_loader.h"
#include "chunk/chunk_writer.h"
#include "types/ulist.h"

namespace ustore {

class SList : public UList {
  friend class ChunkableTypeFactory;
 public:
  SList(SList&&) = default;
  SList& operator=(SList&&) = default;
  ~SList() = default;

  // entry vector can be empty
  Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
              const std::vector<Slice>& entries) const override;

  // Use this List as base to perform three-way merging
  //   return empty hash when merging fails
  Hash Merge(const SList& node1, const SList& node2) const;

 protected:
  // Load existing SList
  SList(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
        const Hash& root_hash) noexcept;
  // create new SList
  SList(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
        const std::vector<Slice>& elements) noexcept;

 private:
  ChunkWriter* chunk_writer_;
};
}  // namespace ustore

#endif  //  USTORE_TYPES_SERVER_SLIST_H_
