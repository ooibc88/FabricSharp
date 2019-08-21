// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_COMPARATOR_H_
#define USTORE_NODE_NODE_COMPARATOR_H_

#include <memory>
#include <utility>
#include <vector>

#include "chunk/chunk_loader.h"
#include "hash/hash.h"
#include "node/cursor.h"
#include "node/meta_node.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

using RangeMaps = std::vector<std::pair<IndexRange, IndexRange>>;
// the following two traits dictate how to compute key
//   from element, metaentry and seqnode for comparing and traversing
struct IndexTrait {
  // return the key of element pointed by the given cursor and that
  //   element index
  static OrderedKey Key(const NodeCursor& cursor, uint64_t idx) {
    return OrderedKey(idx);
  }

  // return the max key of all elements rooted under the given metaentry
  //   idx is the index of first elemnet rooted under me
  static OrderedKey MaxKey(const MetaEntry* me, uint64_t idx) {
    return OrderedKey(idx + me->numElements());
  }

  // return the max key of all elements rooted under the given seqnode
  //   idx is the index of first elemnet rooted under mn
  static OrderedKey MaxKey(const SeqNode* mn, uint64_t idx) {
    return OrderedKey(idx + mn->numElements());
  }

  // The smallest key among all
  static OrderedKey MinKey() {
    return OrderedKey(0);
  }
};

struct OrderedKeyTrait {
  static OrderedKey Key(const NodeCursor& cursor, uint64_t idx) {
    return cursor.currentKey();
  }

  static OrderedKey MaxKey(const MetaEntry* me, uint64_t idx) {
    return me->orderedKey();
  }

  static OrderedKey MaxKey(const SeqNode* mn, uint64_t idx) {
    return mn->key(mn->numEntries() - 1);
  }

  static OrderedKey MinKey() {
    static constexpr byte_t kEMPTY[] = "\0";
    return OrderedKey(false, kEMPTY, 1);
  }
};

// THe following traits specify
//   two procedures during two prolly tree comparing and traversing.
//   The first procedure is performed when encountering the same hashes
//   The second procedure is performed either side reaches the leaf node
template <class KeyTrait>
struct Intersector {
/*Intersector is used to find the index ranges
    of lhs elements that occur in rhs*/
  typedef std::vector<IndexRange> ResultType;
  // Both lhs and rhs share the same elements due to the identical hashes
  //   Return the entire index range of lhs side
  static ResultType IdenticalHashes(
      const SeqNode* lhs, uint64_t lhs_start_idx,
      const SeqNode* rhs, uint64_t rhs_start_idx) {
    std::vector<IndexRange> result;
    result.push_back({lhs_start_idx, lhs->numElements()});
    return result;
  }

  static ResultType IterateLeaves(
      const SeqNode* lhs, uint64_t lhs_start_idx, ChunkLoader* lloader,
      const SeqNode* rhs, uint64_t rhs_start_idx, ChunkLoader* rloader) {
    std::vector<IndexRange> results;

    NodeCursor lhs_cursor(lhs->hash(), 0, lloader);
    NodeCursor rhs_cursor(rhs->hash(), 0, rloader);

    uint64_t lhs_idx = lhs_start_idx;
    uint64_t rhs_idx = rhs_start_idx;

    // if curr_cr.num_subsequent = 0,
    //   this curr_cr is invalid.
    IndexRange curr_cr{0, 0};

    while (!lhs_cursor.isEnd() && !rhs_cursor.isEnd()) {
      OrderedKey lhs_key = KeyTrait::Key(lhs_cursor, lhs_idx);
      OrderedKey rhs_key = KeyTrait::Key(rhs_cursor, rhs_idx);

      if (lhs_key > rhs_key) {
        ++rhs_idx;
        rhs_cursor.Advance(true);
      } else if (lhs_key == rhs_key) {
        size_t lhs_len = lhs_cursor.numCurrentBytes();
        size_t rhs_len = rhs_cursor.numCurrentBytes();

        if (lhs_len == rhs_len &&
            std::memcmp(lhs_cursor.current(),
                        rhs_cursor.current(),
                        lhs_len) == 0) {
        // Identical elements
          if (curr_cr.num_subsequent == 0) {
            curr_cr.start_idx = lhs_idx;
            curr_cr.num_subsequent = 1;
          } else {
            ++curr_cr.num_subsequent;
          }
        } else {
          if (curr_cr.num_subsequent != 0) {
            results.push_back(curr_cr);
            curr_cr.num_subsequent = 0;
          }  // end if
        }  // data comparison

        ++lhs_idx;
        ++rhs_idx;
        lhs_cursor.Advance(true);
        rhs_cursor.Advance(true);
      } else {
        // lhs_idx < rhs_idx
        //   element pointed by lhs cursor
        //   does not appear ih rhs
        if (curr_cr.num_subsequent != 0) {
          results.push_back(curr_cr);
          curr_cr.num_subsequent = 0;
        }  // end if
        ++lhs_idx;
        lhs_cursor.Advance(true);
      }
    }  // end while

    if (curr_cr.num_subsequent != 0) {
      results.push_back(curr_cr);
    }  // end if

    return results;
  }
};

template <class KeyTrait>
struct Differ {
/*Differ is used to find the index ranges of
lhs elements that DOES NOT occur in rhs*/
  typedef std::vector<IndexRange> ResultType;
  // Due to the identical hash, all lhs elements is contained in rhs.
  //   Return empty index range
  static ResultType IdenticalHashes(
      const SeqNode* lhs, uint64_t lhs_start_idx,
      const SeqNode* rhs, uint64_t rhs_start_idx) {
    // Return empty result
      std::vector<IndexRange> result;
      return result;
  }

  static ResultType IterateLeaves(
      const SeqNode* lhs, uint64_t lhs_start_idx, ChunkLoader* lloader,
      const SeqNode* rhs, uint64_t rhs_start_idx, ChunkLoader* rloader) {
    // DLOG(INFO) << "Iterate Diff: \n"
    //            << "LHS: " << lhs << " Start_Idx: " << lhs_start_idx << "\n"
    //            << "RHS: " << rhs << " Start_Idx: " << rhs_start_idx;

    std::vector<IndexRange> results;

    NodeCursor lhs_cursor(lhs->hash(), 0, lloader);
    NodeCursor rhs_cursor(rhs->hash(), 0, rloader);

    uint64_t lhs_idx = lhs_start_idx;
    uint64_t rhs_idx = rhs_start_idx;

    // whether num_subsequent field = 0 in curr_cr to
    //   mark whether this curr_cr is valid or not.
    IndexRange curr_cr{0, 0};

    while (!lhs_cursor.isEnd() && !rhs_cursor.isEnd()) {
      const OrderedKey lhs_key = KeyTrait::Key(lhs_cursor, lhs_idx);
      const OrderedKey rhs_key = KeyTrait::Key(rhs_cursor, rhs_idx);

      if (lhs_key > rhs_key) {
        ++rhs_idx;
        rhs_cursor.Advance(true);
      } else if (lhs_key == rhs_key) {
        size_t lhs_len = lhs_cursor.numCurrentBytes();
        size_t rhs_len = rhs_cursor.numCurrentBytes();

        if (lhs_len == rhs_len &&
            std::memcmp(lhs_cursor.current(),
                        rhs_cursor.current(),
                        lhs_len) == 0) {
        // Identical elements,
        //   Stop updating curr_cr if valid
          if (curr_cr.num_subsequent != 0) {
            results.push_back(curr_cr);
            curr_cr.num_subsequent = 0;  // mark curr_cr is invalid
          }  // end if
        } else {
          if (curr_cr.num_subsequent == 0) {
            // DLOG(INFO) << "Diff Start Idx: "
            //           << lhs_idx;
            curr_cr.start_idx = lhs_idx;
            curr_cr.num_subsequent = 1;
          } else {
            ++curr_cr.num_subsequent;
            // DLOG(INFO) << "  Incrementing for idx " << lhs_idx
            //            << " to " << curr_cr.num_subsequent;
          }
        }  // data comparison

        ++lhs_idx;
        ++rhs_idx;
        lhs_cursor.Advance(true);
        rhs_cursor.Advance(true);
      } else {
        // lhs_idx < rhs_idx
        //   rhs does not contain the element pointed by lhs
        if (curr_cr.num_subsequent == 0) {
          curr_cr.start_idx = lhs_idx;
          curr_cr.num_subsequent = 1;
        } else {
          ++curr_cr.num_subsequent;
        }
        ++lhs_idx;
        lhs_cursor.Advance(true);
      }
    }  // end while

    // if lhs not to the end,
    //   the rest of elements are not contained in rhs
    // Include all of them in index range

    if (!lhs_cursor.isEnd()) {
      if (curr_cr.num_subsequent == 0) {
        curr_cr.start_idx = lhs_idx;
        curr_cr.num_subsequent = 0;
      }  // end if

      do {
        ++curr_cr.num_subsequent;
        // DLOG(INFO) << "  Incrementing for idx " << lhs_idx
        //            << " to " << curr_cr.num_subsequent;
        ++lhs_idx;
      } while (lhs_cursor.Advance(true));  // end while
    }  // end if

    if (curr_cr.num_subsequent != 0) {
      results.push_back(curr_cr);
    }  // end if

    return results;
  }
};

template <class KeyTrait>
struct Mapper {
/*
Mapper is used to map the index range of identical lhs elements to rhs elements
*/
  typedef RangeMaps ResultType;
  // Both lhs and rhs share the same elements due to the identical hashes
  //   Map the entire lhs index range to the entire rhs index range
  static ResultType IdenticalHashes(
      const SeqNode* lhs, uint64_t lhs_start_idx,
      const SeqNode* rhs, uint64_t rhs_start_idx) {
    ResultType result;

    IndexRange lhs_range{lhs_start_idx, lhs->numElements()};
    IndexRange rhs_range{rhs_start_idx, rhs->numElements()};

    result.push_back({lhs_range, rhs_range});
    return result;
  }

  static ResultType IterateLeaves(
      const SeqNode* lhs, uint64_t lhs_start_idx, ChunkLoader* lloader,
      const SeqNode* rhs, uint64_t rhs_start_idx, ChunkLoader* rloader) {
    ResultType results;

    NodeCursor lhs_cursor(lhs->hash(), 0, lloader);
    NodeCursor rhs_cursor(rhs->hash(), 0, rloader);

    uint64_t lhs_idx = lhs_start_idx;
    uint64_t rhs_idx = rhs_start_idx;

    // if lhs_range.num_subsequent = 0,
    //   this lhs_range is invalid.
    IndexRange lhs_range{0, 0};
    IndexRange rhs_range{0, 0};

    while (!lhs_cursor.isEnd() && !rhs_cursor.isEnd()) {
      OrderedKey lhs_key = KeyTrait::Key(lhs_cursor, lhs_idx);
      OrderedKey rhs_key = KeyTrait::Key(rhs_cursor, rhs_idx);

      if (lhs_key > rhs_key) {
        if (lhs_range.num_subsequent != 0) {
          DCHECK_NE(size_t(0), rhs_range.num_subsequent);
          DCHECK(rhs_range.num_subsequent == lhs_range.num_subsequent);
          results.push_back({lhs_range, rhs_range});
          // Mark both ranges as invalid
          lhs_range.num_subsequent = 0;
          rhs_range.num_subsequent = 0;
        }

        ++rhs_idx;
        rhs_cursor.Advance(true);
      } else if (lhs_key == rhs_key) {
        size_t lhs_len = lhs_cursor.numCurrentBytes();
        size_t rhs_len = rhs_cursor.numCurrentBytes();

        if (lhs_len == rhs_len &&
            std::memcmp(lhs_cursor.current(),
                        rhs_cursor.current(),
                        lhs_len) == 0) {
        // Identical elements
          if (lhs_range.num_subsequent == 0) {
            DCHECK_EQ(size_t(0), rhs_range.num_subsequent);
            lhs_range.start_idx = lhs_idx;
            lhs_range.num_subsequent = 1;

            rhs_range.start_idx = rhs_idx;
            rhs_range.num_subsequent = 1;
          } else {
            DCHECK_NE(size_t(0), lhs_range.num_subsequent);
            ++lhs_range.num_subsequent;
            ++rhs_range.num_subsequent;
          }
        } else {
          if (lhs_range.num_subsequent != 0) {
            DCHECK_NE(size_t(0), rhs_range.num_subsequent);
            DCHECK(rhs_range.num_subsequent == lhs_range.num_subsequent);
            results.push_back({lhs_range, rhs_range});
            // Mark both ranges as invalid
            lhs_range.num_subsequent = 0;
            rhs_range.num_subsequent = 0;
          }
        }  // data comparison

        ++lhs_idx;
        ++rhs_idx;
        lhs_cursor.Advance(true);
        rhs_cursor.Advance(true);
      } else {
        if (lhs_range.num_subsequent != 0) {
          DCHECK_NE(size_t(0), rhs_range.num_subsequent);
          DCHECK(rhs_range.num_subsequent == lhs_range.num_subsequent);
          results.push_back({lhs_range, rhs_range});
          // Mark both ranges as invalid
          lhs_range.num_subsequent = 0;
          rhs_range.num_subsequent = 0;
        }
        ++lhs_idx;
        lhs_cursor.Advance(true);
      }
    }  // end while

    if (lhs_range.num_subsequent != 0) {
      DCHECK_NE(size_t(0), rhs_range.num_subsequent);
      DCHECK(rhs_range.num_subsequent == lhs_range.num_subsequent);

      results.push_back({lhs_range, rhs_range});
      // Mark both ranges as invalid
      lhs_range.num_subsequent = 0;
      rhs_range.num_subsequent = 0;
    }

    return results;
  }
};

template <class KeyTrait, template<class> class Traverser>
class NodeComparator : private Noncopyable {
/*
NodeComparator compares two prolly trees using the following procedure:

For each lhs node in pre-order tranversal:
  Find the deepest rhs node which must contain all the lhs node element;
  If lhs and rhs node share the same hash:
    Perform the procedure specified in Traverser
    return;

  If lhs or rhs node reaches the leaf:
    Perform the procedure specified in Traverser
    return;

  Recursive perform the above steps for each lhs child node


KeyTrait specifies the key for traversing, either can be prolly index or orderedkey
*/
 public:
  using ReturnType = typename Traverser<KeyTrait>::ResultType;
  NodeComparator(const Hash& rhs,
                 ChunkLoader* rloader) noexcept;

  virtual ~NodeComparator() = default;

  // if lloader is not provided (nullptr),
  //   rloader_ is used to load chunks for lhs.
  ReturnType Compare(const Hash& lhs, ChunkLoader* lloader = nullptr) const;

 private:
  // This function starts to search for this deepest seq node from rhs_seq_node
  std::shared_ptr<const SeqNode> SmallestOverlap(
      const OrderedKey& lhs_lower, const OrderedKey& lhs_upper,
      std::shared_ptr<const SeqNode> rhs_seq_node,
      uint64_t rhs_start_idx, uint64_t* return_start_idx) const;

  // Compare the lhs tree with rhs in preorder format
  ReturnType Compare(
      const SeqNode* lhs, uint64_t lhs_start_idx, const OrderedKey& lhs_min_key,
      ChunkLoader* lloader, std::shared_ptr<const SeqNode> rhs_root_node,
      uint64_t rhs_start_idx) const;

  // loader for both lhs and rhs
  mutable ChunkLoader* rloader_;

  std::shared_ptr<const SeqNode> rhs_root_;
};

template <class KeyTrait, template<class> class Traverser>
NodeComparator<KeyTrait, Traverser>::NodeComparator(const Hash& rhs,
    ChunkLoader* rloader) noexcept : rloader_(rloader) {
  const Chunk* chunk = rloader_->Load(rhs);
  rhs_root_ = SeqNode::CreateFromChunk(chunk);
}

template <class KeyTrait, template<class> class Traverser>
typename NodeComparator<KeyTrait, Traverser>::ReturnType
  NodeComparator<KeyTrait, Traverser> ::Compare(const Hash& lhs,
                                                ChunkLoader* lloader) const {
  if (lloader == nullptr) {
    lloader = rloader_;
  }
  std::unique_ptr<const SeqNode> lhs_root =
        SeqNode::CreateFromChunk(lloader->Load(lhs));

  uint64_t lhs_start_idx = 0;
  uint64_t rhs_start_idx = 0;

  return IndexRange::Compact(Compare(lhs_root.get(),
                             lhs_start_idx,
                             KeyTrait::MinKey(),
                             lloader,
                             rhs_root_,
                             rhs_start_idx));
}

template <class KeyTrait, template<class> class Traverser>
typename NodeComparator<KeyTrait, Traverser>::ReturnType
  NodeComparator<KeyTrait, Traverser>::Compare(
    const SeqNode* lhs, uint64_t lhs_start_idx, const OrderedKey& lhs_min_key,
    ChunkLoader* lloader,
    const std::shared_ptr<const SeqNode> rhs_node,
    uint64_t rhs_start_idx) const {
  // rhs_root_node is guaranteed to contain all the elements rooted in lhs
  ReturnType results;

  // DLOG(INFO) << "Start Comparing LHS and RHS: \n"
  //            << "LHS Hash: " << lhs->hash().ToBase32()
  //            << " Start Idx: " << lhs_start_idx << "\n"
  //            << "RHS Hash: " << rhs_node->hash().ToBase32()
  //            << " Start Idx: " << rhs_start_idx << "\n";

  // uint64_t lower = lhs_start_idx;
  // uint64_t upper = lhs_start_idx + lhs->numElements();

  // the index of the first element rooted at rhs_closest_node
  uint64_t deepest_start_idx = 0;
  // share_ptr to seqnode
  auto rhs_deepest_node = SmallestOverlap(lhs_min_key,
                                          KeyTrait::MaxKey(lhs, lhs_start_idx),
                                          rhs_node,
                                          rhs_start_idx,
                                          &deepest_start_idx);

  // DLOG(INFO) << "Closest Overlap: \n"
  //            << "Hash: " << rhs_closest_node->hash()
  //            << " Start Idx: " << closest_start_idx;
  if (lhs->hash() == rhs_deepest_node->hash()) {
    return Traverser<KeyTrait>::IdenticalHashes(lhs, lhs_start_idx,
                                                rhs_deepest_node.get(),
                                                deepest_start_idx);
  }

  if (lhs->isLeaf() || rhs_deepest_node->isLeaf()) {
    return Traverser<KeyTrait>::IterateLeaves(lhs, lhs_start_idx, lloader,
                                              rhs_deepest_node.get(),
                                              deepest_start_idx,
                                              rloader_);
  }  // end if

  // Preorder Traversal
  uint64_t lhs_child_start_idx = lhs_start_idx;
  OrderedKey lhs_child_min_key = lhs_min_key;

  const MetaNode* lhs_meta = dynamic_cast<const MetaNode*>(lhs);
  for (size_t i = 0; i < lhs->numEntries(); i++) {
    MetaEntry lhs_me(lhs_meta->data(i));

    std::unique_ptr<const SeqNode> lhs_child_node =
        SeqNode::CreateFromChunk(lloader->Load(lhs_me.targetHash()));

    ReturnType child_results =
        Compare(lhs_child_node.get(),
                lhs_child_start_idx,
                lhs_child_min_key,
                lloader,
                rhs_deepest_node,
                deepest_start_idx);

// Concat child_results at the end of final results by moving
    results.insert(results.end(), child_results.begin(), child_results.end());

// Prepare for next iteration
    lhs_child_min_key = KeyTrait::MaxKey(lhs_child_node.get(),
                                          lhs_child_start_idx);

    lhs_child_start_idx += lhs_me.numElements();
  }
  return results;
}

template <class KeyTrait, template<class> class Traverser>
std::shared_ptr<const SeqNode>
NodeComparator<KeyTrait, Traverser>::SmallestOverlap(
                                            const OrderedKey& lhs_lower,
                                            const OrderedKey& lhs_upper,
                                            std::shared_ptr<const SeqNode>
                                                rhs_node,
                                            uint64_t rhs_start_idx,
                                            uint64_t* closest_start_idx) const {
  // Precondition, rhs_node shall at least contain the range
  // CHECK_LE(rhs_start_idx, lhs_lower);
  if (!rhs_node->isLeaf()) {
    const MetaNode* meta_node = dynamic_cast<const MetaNode*>(rhs_node.get());

    size_t numEntries = meta_node->numEntries();

    // accumalated start idx for each metaentry
    uint64_t rhs_me_start_idx = rhs_start_idx;

    OrderedKey rhs_me_lower;  // me is in short for metaentry
    OrderedKey rhs_me_upper;

    for (size_t i = 0; i < numEntries; ++i) {
      MetaEntry rhs_me(meta_node->data(i));

      // no further meta_entry will contain this range
      if (i > 0 && rhs_me_lower > lhs_lower) break;

      rhs_me_upper = KeyTrait::MaxKey(&rhs_me, rhs_me_start_idx);

      if (i == numEntries - 1 || rhs_me_upper >= lhs_upper) {
        // Find a deeper overlap
        const Chunk* rhs_child_chunk = rloader_->Load(rhs_me.targetHash());
        std::shared_ptr<const SeqNode> rhs_child =
                         SeqNode::CreateFromChunk(rhs_child_chunk);

        return SmallestOverlap(lhs_lower,
                               lhs_upper,
                               rhs_child,
                               rhs_me_start_idx,
                               closest_start_idx);
      }
      rhs_me_start_idx += rhs_me.numElements();
      rhs_me_lower = rhs_me_upper;
    }  // end for
  }  // end if

  *closest_start_idx = rhs_start_idx;
  return rhs_node;
}

using IndexIntersector = NodeComparator<IndexTrait, Intersector>;

using IndexDiffer = NodeComparator<IndexTrait, Differ>;

using KeyIntersector = NodeComparator<OrderedKeyTrait, Intersector>;

using KeyDiffer = NodeComparator<OrderedKeyTrait, Differ>;

using KeyMapper = NodeComparator<OrderedKeyTrait, Mapper>;


class LevenshteinMapper : private Noncopyable {
/*
 LevenshteinMapper maps the elements bewtween two prolly trees based on the minimum
 edit distance.

*/
 public:
// loader is used for both lhs and rhs
  LevenshteinMapper(const Hash& rhs,
                    ChunkLoader* rloader) noexcept :
      rhs_(rhs), rloader_(rloader) {}

  virtual ~LevenshteinMapper() = default;

  // Return the index maps from lhs to rhs
  RangeMaps Compare(const Hash& lhs, ChunkLoader* lloader = nullptr) const;

 private:
  enum class EditMarker : unsigned char {
    kNull = 0,
    kLHSInsertion = 1,
    kRHSInsertion = 2,
    kSubstitution = 3,
    kMatch = 4
  };

  struct Entry {
    EditMarker marker;
    uint64_t edit_distance;
  };
  // Map the elements in recursive manner from top to bottom
  // Args:
  //   lhs_cr: cursor pointing to the first entry of lhs
  //   lhs_range: the lhs index range for mapping
  //   rhs_cr: cursor pointing to the first entry of rhs
  //   rhs_range: the rhs index range for mapping
  RangeMaps map(std::unique_ptr<NodeCursor> lhs_cr, IndexRange lhs_range,
                std::unique_ptr<NodeCursor> rhs_cr, IndexRange rhs_range) const;

  // Map the elements in two sequececes based on minimum edit distance.
  // Args:
  //   lhs_cr: cursor pointing to the first entry of lhs
  //   lhs_range: the lhs index range for mapping
  //   rhs_cr: cursor pointing to the first entry of rhs
  //   rhs_range: the rhs index range for mapping
  RangeMaps SeqMap(std::unique_ptr<NodeCursor> lhs_cr, IndexRange lhs_range,
      std::unique_ptr<NodeCursor> rhs_cr, IndexRange rhs_range) const;

  // return the number of elements rooted at the entry pointed by cursor
  static uint64_t numElementsByCursor(const NodeCursor& cursor);

  const Hash rhs_;
  // loader for both lhs and rhs
  mutable ChunkLoader* rloader_;
};
}  // namespace ustore

#endif  // USTORE_NODE_NODE_COMPARATOR_H_
