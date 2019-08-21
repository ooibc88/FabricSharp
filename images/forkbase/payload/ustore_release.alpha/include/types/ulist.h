// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_ULIST_H_
#define USTORE_TYPES_ULIST_H_

#include <memory>
#include <vector>
#include <utility>

#include "node/list_node.h"
#include "types/base.h"
#include "types/uiterator.h"

namespace ustore {

class UList : public ChunkableType {
 public:
  class Iterator : public CursorIterator {
    friend class UList;
   public:
    inline Slice key() const override {
      LOG(WARNING) << "Key not supported for list";
      return Slice();
    }

   private:
    // Only used by UList
    Iterator(const Hash& root, const std::vector<IndexRange>& ranges,
             ChunkLoader* loader) noexcept :
      CursorIterator(root, ranges, loader) {}

    // Only used by UList
    Iterator(const Hash& root, std::vector<IndexRange>&& ranges,
             ChunkLoader* loader) noexcept :
      CursorIterator(root, std::move(ranges), loader) {}

    inline Slice RealValue() const override {
      return ListNode::Decode(data());
    }
  };

  static DuallyDiffIndexIterator DuallyDiff(const UList& lhs, const UList& rhs);

  // For idx > total # of elements
  //    return empty slice
  Slice Get(uint64_t idx) const;
  // entry vector can be empty
  // TODO(pingcheng): cannot use size_t instead?
  virtual Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
                      const std::vector<Slice>& entries) const = 0;
  Hash Delete(uint64_t start_idx, uint64_t num_to_delete) const;
  Hash Insert(uint64_t start_idx, const std::vector<Slice>& entries) const;
  Hash Append(const std::vector<Slice>& entries) const;
  // Return an iterator that scan from List Start
  UList::Iterator Scan() const;
  // Return an iterator that scan elements that exist in this Ulist
  //   and NOT in rhs
  UList::Iterator Diff(const UList& rhs) const;
  // Return an iterator that scan elements that both exist in this Ulist and rhs
  UList::Iterator Intersect(const UList& rhs) const;

  friend std::ostream& operator<<(std::ostream& os, const UList& obj);

 protected:
  UList() = default;
  UList(UList&&) = default;
  UList& operator=(UList&&) = default;
  // create an empty map
  // construct chunk loader for server
  explicit UList(std::shared_ptr<ChunkLoader> loader) noexcept :
    ChunkableType(loader) {}
  // construct chunk loader for server
  ~UList() = default;

  bool SetNodeForHash(const Hash& hash) override;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_ULIST_H_
