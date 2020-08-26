// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_USET_H_
#define USTORE_TYPES_USET_H_

#include <memory>
#include <utility>
#include <vector>

#include "node/set_node.h"
#include "types/base.h"
#include "types/uiterator.h"

namespace ustore {

class USet : public ChunkableType {
 public:
  static DuallyDiffKeyIterator DuallyDiff(const USet& lhs, const USet& rhs);

  class Iterator : public CursorIterator {
    friend class USet;
   public:
    inline uint64_t index() const override {
      LOG(WARNING) << "Index not supported for Set";
      return 0;
    }

   private:
    // Only used by USet
    Iterator(const Hash& root, const std::vector<IndexRange>& ranges,
             ChunkLoader* loader) noexcept
      : CursorIterator(root, ranges, loader) {}

    // Only used by USet
    Iterator(const Hash& root, std::vector<IndexRange>&& ranges,
             ChunkLoader* loader) noexcept
      : CursorIterator(root, std::move(ranges), loader) {}

    inline Slice RealValue() const override {
      size_t key_num_bytes = 0;
      const char* key = reinterpret_cast<const char*>(
                              SetNode::key(data(), &key_num_bytes));
      return Slice(key, key_num_bytes);
    }
  };

  // Use chunk loader to load chunk and read value
  // return empty slice if key not found
  Slice Get(const Slice& key) const;
  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  virtual Hash Set(const Slice& key) const = 0;
  virtual Hash Remove(const Slice& key) const = 0;
  // Return an iterator that scan from List Start
  USet::Iterator Scan() const;
  // Return an iterator that scan elements that exist in this USet
  //   and NOT in rhs
  USet::Iterator Diff(const USet& rhs) const;
  // Return an iterator that scan elements that both exist in this USet and rhs
  USet::Iterator Intersect(const USet& rhs) const;

  friend std::ostream& operator<<(std::ostream& os, const USet& obj);

 protected:
  USet() = default;
  USet(USet&&) = default;
  USet& operator=(USet&&) = default;
  explicit USet(std::shared_ptr<ChunkLoader> loader) noexcept :
      ChunkableType(loader) {}
  ~USet() = default;

  bool SetNodeForHash(const Hash& hash) override;
};

}  // namespace ustore

#endif  // USTORE_TYPES_USET_H_
