// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CURSOR_H_
#define USTORE_NODE_CURSOR_H_

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>


#include "chunk/chunk_loader.h"
#include "node/orderedkey.h"
#include "node/node.h"
#include "types/type.h"

namespace ustore {

struct IndexRange {
  uint64_t start_idx;
  uint64_t num_subsequent;

// Compact continuous index ranges to single one, e.g,
//  {start_idx, num_subsequent}
//   {0, 3} + {3, 6} -> {0, 9}
  static std::vector<IndexRange> Compact(const std::vector<IndexRange>& ranges);

// Compact continuous index range maps to a single one, e.g.
// {0, 2} => {3, 2} + {2, 4} => {5, 4}
//   {0, 6} => {3, 6}
  static std::vector<std::pair<IndexRange, IndexRange>> Compact(
      const std::vector<std::pair<IndexRange, IndexRange>>& range_map);

  static std::string to_str(IndexRange range) {
    std::ostringstream sstream;
    sstream << "(" << range.start_idx << ", "
               << range.num_subsequent << ") ";
    return sstream.str();
  }

  static std::string to_str(std::pair<IndexRange, IndexRange> range_map) {
    std::ostringstream sstream;
    sstream << "(" << range_map.first.start_idx << ", "
                      << range_map.first.num_subsequent << ")"
            << " => "
            << "(" << range_map.second.start_idx << ", "
                   << range_map.second.num_subsequent << ")";
    return sstream.str();
  }
};

class NodeCursor {
 public:
  // Init Cursor to point at idx element at leaf in a tree
  // rooted at SeqNode with Hash
  // if idx == total_num elemnets,
  //   cursor points to the end of sequence.
  // if idx > total_num_elements
  //   return nullptr
  NodeCursor(const Hash& hash, size_t idx, ChunkLoader* ch_loader) noexcept;
  NodeCursor(const Hash& hash, const OrderedKey& key,
             ChunkLoader* ch_loader) noexcept;
  // Copy constructor used to clone a NodeCursor
  // Need to recursively copy the parent NodeCursor
  NodeCursor(const NodeCursor& cursor) noexcept;
  // move ctor
  NodeCursor(NodeCursor&&) = default;
  NodeCursor& operator=(NodeCursor&&) = default;
  ~NodeCursor() = default;

  // Advance the pointer by one element,
  // Allow to cross the boundary and advance to the start of next node
  // @return Return whether the cursor has already reached the end after the
  // operation
  //   1. False if cross_boundary = true and idx = numElements()
  //     of the last leaf node of the entire tree
  //   2. False if cross_boundary = false and idx = numElements()
  //     of the pointed leaf node
  bool Advance(bool cross_boundary);

  // Retreate the pointer by one element,
  // Allow to cross the boundary and point to the last element of preceding node
  // @return Return whether the cursor has already reached the first element
  //   1. False if cross_boundary = true and cursor points the very first
  //   element
  //   2. False if cross_boundary = false and cursor points the node's first
  //   element
  bool Retreat(bool cross_boundary);

  // Advance entries in this cursor pointed node
  //   May bypass to the next chunk
  // Return the actual number of entries advanced
  size_t AdvanceEntry(size_t num_entry);

  // Retreat entries in this cursor pointed node
  //   May bypass to the previous chunk
  // Return the actual number of entries retreated
  size_t RetreatEntry(size_t num_entry);

  inline OrderedKey currentKey() const { return seq_node_->key(idx_); }

  // Advance skip multiple elements.
  // Possible to cross boundary for advancement
  // return the number of actual advancement
  uint64_t AdvanceSteps(uint64_t step);

  // Retreat skip multiple elements.
  // Possible to cross boundary for retreating
  // return the number of actual retreat
  uint64_t RetreatSteps(uint64_t step);

  // return the data pointed by current cursor
  const byte_t* current() const;
  // return the number of bytes of pointed element
  size_t numCurrentBytes() const;

  // two cursor are equal if the following condition are ALL met:
  //   same idx
  //   point to the same seqnode
  //   same parent cursor
  bool operator==(const NodeCursor& rhs) const;

  // cursor places at seq end
  inline bool isEnd() const { return idx_ == int32_t(seq_node_->numEntries()); }
  // cursor places at seq start
  inline bool isBegin() const { return idx_ == -1; }

  inline NodeCursor* parent() const { return parent_cr_.get(); }

  // value is -1 when pointing to seq start
  inline int32_t idx() const { return idx_; }

  // move the pointer to point to idx entry
  inline void seek(int32_t idx) {
    CHECK_LE(0, idx);
    CHECK_GE(int32_t(seq_node_->numEntries()), idx);
    idx_ = idx;
  }

  inline const SeqNode* node() const { return seq_node_.get(); }

  inline bool empty() const { return !seq_node_.get(); }

  inline ChunkLoader* loader() const { return chunk_loader_; }

 private:
  // Init cursor given parent cursor
  // Internally use to create NodeCursor recursively
  // TODO(wangji/pingcheng): check if really need to share SeqNode
  NodeCursor(std::shared_ptr<const SeqNode> seq_node, size_t idx,
             ChunkLoader* chunk_loader, std::unique_ptr<NodeCursor> parent_cr);

  std::unique_ptr<NodeCursor> parent_cr_;
  // the pointed sequence
  std::shared_ptr<const SeqNode> seq_node_;
  ChunkLoader* chunk_loader_;
  // the index of pointed elements
  // can be -1 when pointing to seq start
  int32_t idx_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CURSOR_H_
