// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_MERGER_H_
#define USTORE_NODE_NODE_MERGER_H_

#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "chunk/chunk_loader.h"
#include "hash/hash.h"
#include "node/cursor.h"
#include "node/node_comparator.h"
#include "node/node_builder.h"
#include "utils/noncopyable.h"

namespace ustore {

struct OrderedKeyMapper {
  // Map the index range of lhs elements to the index range
  //   of identical elements in rhs based on key comparison
  // Refer to KeyMapper and corresponding test cases for details
  static RangeMaps Map(const Hash& lhs, const Hash& rhs, ChunkLoader* loader) {
    KeyMapper mapper(rhs, loader);
#ifdef DEBUG
  RangeMaps result = mapper.Compare(lhs);
  for (auto map_it = result.begin(); map_it != result.end();
       ++map_it) {
    IndexRange lhs_range = map_it->first;
    IndexRange rhs_range = map_it->second;

    DLOG(INFO) << "Result from OrderedKeyMapper: "
               << "(" << lhs_range.start_idx << ", "
               <<  lhs_range.num_subsequent << ") => "
               << "(" << rhs_range.start_idx << ", "
               <<  rhs_range.num_subsequent << ")";
  }
  return result;
#else
  return mapper.Compare(lhs);
#endif
  }
};

struct MinEditDistanceMapper {
  // Map the index range of lhs elements to the index range
  //   of identical elements in rhs based on edit distance
  static RangeMaps Map(const Hash& lhs, const Hash& rhs, ChunkLoader* loader) {
    LevenshteinMapper mapper(rhs, loader);
#ifdef DEBUG
  RangeMaps result = mapper.Compare(lhs);
  for (auto map_it = result.begin(); map_it != result.end();
       ++map_it) {
    IndexRange lhs_range = map_it->first;
    IndexRange rhs_range = map_it->second;

    DLOG(INFO) << "Results from MinEditDistanceMapper: "
               << "(" << lhs_range.start_idx << ", "
               <<  lhs_range.num_subsequent << ") => "
               << "(" << rhs_range.start_idx << ", "
               <<  rhs_range.num_subsequent << ")";
  }
  return result;
#else
  return mapper.Compare(lhs);
#endif
  }
};

template <class Mapper>
class NodeMerger : private Noncopyable {
/* NodeMerger performs the three-way merging based on the following algorithm

For each node to be merge:
  Compare it with base to get index range maps of identical elements
  (How to map is specified by mapper.)
  Flip the index range maps to get the different part

Edit the base node based on the different parts of merged nodes sequentially

e.g,

Base: A, B, C, D, F
Node 1: A, B, D, E, F
Node 2: B, C, D, E, F

The mapping ({index, length}) between base and node1 is:
{0, 2} -> {0, 2}, {3, 1} -> {2, 1}, {4, 1} -> {4, 1}
  The flipped mapping is: {2, 1} -> {2, 0}, {4, 0}-> {3, 1}

Similarly, the flipped mapping between base and node2 is:
{0, 1} -> {0, 0}, {4, 0} -> {3, 1}

The constructed merged node is constructed from base node by sequentially
  replace the first element with empty (Remove A),
  replace the second element with empty (Remove C),
  insert at 4th position with the third element of Node 1 (Insert E)
  (insert at 4th position with the third element of Node 2 (Insert E)).

Hence, the final result is B, C, D, E, F.

If the working ranges of base overlaps and the applied difference is NOT the same,
the merging fails.


Usage:
  NodeMerger merger(baseHash, loader);
  Hash result_hash = merger.Merge(node1_hash, node2_hash);
*/
 public:
  // Node: loader is used for base, node1 and node2
  NodeMerger(const Hash& base, ChunkLoader* loader,
             ChunkWriter* writer) noexcept :
    base_(base), loader_(loader), writer_(writer) {}

  virtual ~NodeMerger() = default;

  // Return empty hash if merging fails.
  Hash Merge(const Hash& node1, const Hash& node2,
             const Chunker& chunker) const;

 private:
  // Flip the index range maps of identical elements
  //   to get the index range maps of differenet parts
  RangeMaps flip(const RangeMaps& range_maps,
                 uint64_t lhs_num_elements,
                 uint64_t rhs_num_elements) const;


  bool IsIdenticalContents(const Hash& lhs, IndexRange lhs_range,
                           const Hash& rhs, IndexRange rhs_range) const;

  uint64_t numElements(const Hash& hash) const;


  std::vector<std::unique_ptr<const Segment>>
      GetSegmentsFromIndexRange(const Hash node,
                                IndexRange index_range) const;

  const Hash base_;
  mutable ChunkLoader* loader_;
  mutable ChunkWriter* writer_;
};

template <class Mapper>
RangeMaps NodeMerger<Mapper>::flip(const RangeMaps& range_maps,
    uint64_t lhs_num_elements, uint64_t rhs_num_elements) const {

#ifdef DEBUG
  DLOG(INFO) << "Flipping function: ";
  DLOG(INFO) << "LHS # of Elements: " << lhs_num_elements
             << " RHS # of Elements: " << rhs_num_elements;

  for (auto range_map_it = range_maps.begin();
       range_map_it != range_maps.end(); ++range_map_it) {
    IndexRange lhs_range = range_map_it->first;
    IndexRange rhs_range = range_map_it->second;
    DLOG(INFO) << IndexRange::to_str({lhs_range, rhs_range});
  }

#endif

  RangeMaps flipped_maps;

  if (range_maps.size() == 0) {
    IndexRange entire_lhs{0, lhs_num_elements};
    IndexRange entire_rhs{0, rhs_num_elements};
    flipped_maps.push_back({entire_lhs, entire_rhs});
    return flipped_maps;
  }

  uint64_t pre_lhs_upper_idx = 0;
  uint64_t pre_rhs_upper_idx = 0;

  for (auto range_map_it = range_maps.begin();
       range_map_it != range_maps.end(); ++range_map_it) {
    IndexRange lhs_range = range_map_it->first;
    IndexRange rhs_range = range_map_it->second;

    DCHECK_LE(pre_lhs_upper_idx, lhs_range.start_idx);
    DCHECK_LE(pre_rhs_upper_idx, rhs_range.start_idx);

    IndexRange pre_lhs_range{pre_lhs_upper_idx,
        lhs_range.start_idx - pre_lhs_upper_idx};

    IndexRange pre_rhs_range{pre_rhs_upper_idx,
        rhs_range.start_idx - pre_rhs_upper_idx};

    if (pre_lhs_range.num_subsequent != 0 ||
        pre_rhs_range.num_subsequent != 0) {
      DLOG(INFO) << "Adding Flipped Range Maps: "
                 << IndexRange::to_str({pre_lhs_range, pre_rhs_range});

      flipped_maps.push_back({pre_lhs_range, pre_rhs_range});
    }

    pre_lhs_upper_idx = lhs_range.start_idx + lhs_range.num_subsequent;
    pre_rhs_upper_idx = rhs_range.start_idx + rhs_range.num_subsequent;
  }

  IndexRange last_lhs_range{pre_lhs_upper_idx,
      lhs_num_elements - pre_lhs_upper_idx};

  IndexRange last_rhs_range{pre_rhs_upper_idx,
      rhs_num_elements - pre_rhs_upper_idx};

  if (last_lhs_range.num_subsequent != 0 ||
      last_rhs_range.num_subsequent != 0) {
    DLOG(INFO) << "Adding Last Flipped Range Maps: "
               << IndexRange::to_str({last_lhs_range, last_rhs_range});

    flipped_maps.push_back({last_lhs_range, last_rhs_range});
  }

  return flipped_maps;
}

template <class Mapper>
bool NodeMerger<Mapper>::IsIdenticalContents(
    const Hash& lhs, IndexRange lhs_range,
    const Hash& rhs, IndexRange rhs_range) const {
  if (lhs_range.num_subsequent != rhs_range.num_subsequent) {
    return false;
  }

  DLOG(INFO) << "Checking Identical Contents for "
             << IndexRange::to_str({lhs_range, rhs_range});

  auto lhs_segs = GetSegmentsFromIndexRange(lhs, lhs_range);
  auto rhs_segs = GetSegmentsFromIndexRange(rhs, rhs_range);

  // Compare concatenated memory of segments from both side are identical or not
  auto lhs_seg_it = lhs_segs.begin();
  auto rhs_seg_it = rhs_segs.begin();

  size_t lhs_idx = 0;
  size_t rhs_idx = 0;

  bool is_identical = true;

  while (lhs_seg_it != lhs_segs.end() &&
         rhs_seg_it != rhs_segs.end()) {
    size_t lhs_remaining_len = (*lhs_seg_it)->numBytes() - lhs_idx;
    size_t rhs_remaining_len = (*rhs_seg_it)->numBytes() - rhs_idx;

    if (lhs_remaining_len < rhs_remaining_len) {
      if (std::memcmp((*lhs_seg_it)->data(),
                      (*lhs_seg_it)->data(),
                      lhs_remaining_len) != 0) {
        is_identical = false;
        break;
      }
      ++lhs_seg_it;
      lhs_idx = 0;
      rhs_idx += lhs_remaining_len;
    } else if (lhs_remaining_len == rhs_remaining_len) {
      if (std::memcmp((*lhs_seg_it)->data(),
                      (*lhs_seg_it)->data(),
                      rhs_remaining_len) != 0) {
        is_identical = false;
        break;
      }
      ++lhs_seg_it;
      lhs_idx = 0;
      ++rhs_seg_it;
      rhs_idx = 0;
    } else {
      if (std::memcmp((*lhs_seg_it)->data(),
                      (*lhs_seg_it)->data(),
                      rhs_remaining_len) != 0) {
        is_identical = false;
        break;
      }
      ++rhs_seg_it;
      rhs_idx = 0;
      lhs_idx += rhs_remaining_len;
    }
  }

  // CHECK both iterator reaches the end at the same time
  #ifdef DEBUG
  if (is_identical) {
    DCHECK(lhs_seg_it == lhs_segs.end() && rhs_seg_it == rhs_segs.end());
    DCHECK(lhs_idx == 0 && rhs_idx == 0);
  }
  #endif

  return is_identical;
}

template <class Mapper>
std::vector<std::unique_ptr<const Segment>>
    NodeMerger<Mapper>::GetSegmentsFromIndexRange(
    const Hash hash, IndexRange index_range) const {
  std::vector<std::unique_ptr<const Segment>> segs;
  DLOG(INFO) << "Get Segments from index: "
             << index_range.start_idx
             << " len: " << index_range.num_subsequent;

  NodeCursor cursor(hash, index_range.start_idx, loader_);

  uint64_t accumulated_num_elements = 0;

  while (true) {
    const LeafNode* leaf_node = dynamic_cast<const LeafNode*>(cursor.node());
    size_t entry_idx = static_cast<size_t>(cursor.idx());
    size_t leaf_remaining_num_elements = static_cast<size_t>(
      leaf_node->numEntries() - cursor.idx());  // inclusive of the pointed one

    uint64_t remaining_len =
      static_cast<size_t>(index_range.num_subsequent
                          - accumulated_num_elements);

    DLOG(INFO) << "Cursor Entry Idx: " << entry_idx
               << " Leaf # of remaining elements: "
               << leaf_remaining_num_elements
               << " Remaining Len: " << remaining_len;

    if (leaf_remaining_num_elements < remaining_len) {
      DLOG(INFO) << "Put segment at entry idx: " << entry_idx
                 << " len: " << leaf_remaining_num_elements;
      std::unique_ptr<const Segment> seg =
        leaf_node->GetSegment(entry_idx, leaf_remaining_num_elements);
      segs.push_back(std::move(seg));

      // Move the cursor to the start of next chunk.
      uint64_t actual_steps = cursor.AdvanceSteps(
          static_cast<uint64_t>(leaf_remaining_num_elements));

      CHECK_EQ(int32_t(0), cursor.idx());
      CHECK_EQ(actual_steps, leaf_remaining_num_elements);
      accumulated_num_elements += leaf_remaining_num_elements;
    } else {
      DLOG(INFO) << "Put last segment at entry idx: " << entry_idx
                 << " len: " << remaining_len;
      std::unique_ptr<const Segment> seg
        = leaf_node->GetSegment(entry_idx, remaining_len);
      segs.push_back(std::move(seg));
      break;
    }
  }  // end of while true

  return segs;
}

template <class Mapper>
uint64_t NodeMerger<Mapper>::numElements(const Hash& hash) const {
  const Chunk* chunk = loader_->Load(hash);
  return SeqNode::CreateFromChunk(chunk)->numElements();
}

template <class Mapper>
Hash NodeMerger<Mapper>::Merge(const Hash& node1, const Hash& node2,
                               const Chunker& chunker) const {
  uint64_t base_num_elements = numElements(base_);
  uint64_t node1_num_elements = numElements(node1);
  uint64_t node2_num_elements = numElements(node2);

  DLOG(INFO) << "Base Number of elements: " << base_num_elements;

  DLOG(INFO) << "Flipping Range Map for Node 1 with "
             << node1_num_elements << " elements.";
  RangeMaps range_map1 = flip(Mapper::Map(base_, node1, loader_),
                              base_num_elements, node1_num_elements);
  DLOG(INFO) << "Flipping Range Map for Node 2 with "
             << node2_num_elements << " elements.";
  RangeMaps range_map2 = flip(Mapper::Map(base_, node2, loader_),
                              base_num_elements, node2_num_elements);

  auto map_it1  = range_map1.begin();
  auto map_it2  = range_map2.begin();

  ustore::AdvancedNodeBuilder builder(base_, loader_, writer_);

#ifdef DEBUG
  bool merge_first_node = false;
#endif

  while (map_it1 != range_map1.end() ||
         map_it2 != range_map2.end()) {
    // Determine which node's difference shall be applied
    DLOG(INFO) << "--------------------------------------------";
    IndexRange base_range;
    IndexRange node_range;
    std::vector<std::unique_ptr<const Segment>> segs;
    if (map_it1 == range_map1.end()) {
      base_range = map_it2->first;
      node_range = map_it2->second;

#ifdef DEBUG
      DLOG(INFO) << "Only Node 2 Map "
                 << IndexRange::to_str({base_range, node_range});

      merge_first_node = false;
#endif
      segs = GetSegmentsFromIndexRange(node2, node_range);
      ++map_it2;
    } else if (map_it2 == range_map2.end()) {
      base_range = map_it1->first;
      node_range = map_it1->second;
#ifdef DEBUG
      DLOG(INFO) << "Only Node 1 Map "
                 << IndexRange::to_str({base_range, node_range});

      merge_first_node = true;
#endif
      segs = GetSegmentsFromIndexRange(node1, node_range);
      ++map_it1;
    } else {
      IndexRange base_range1 = map_it1->first;
      IndexRange base_range2 = map_it2->first;

      IndexRange node_range1 = map_it1->second;
      IndexRange node_range2 = map_it2->second;


      DLOG(INFO) << "LHS Map: "
                 << IndexRange::to_str({base_range, node_range1})
                 << "  "
                 << "RHS Map: "
                 << IndexRange::to_str({base_range, node_range2});

      if (base_range1.start_idx ==
                   base_range2.start_idx &&
                 base_range1.num_subsequent ==
                   base_range2.num_subsequent &&
                 IsIdenticalContents(node1, node_range1,
                                      node2, node_range2)) {
        // base_range1 and base_range2 precisely overlaps
        // and the referencing contents are identical
        //   Apply only one different part

#ifdef DEBUG
        DLOG(INFO) << "Identical Contents. ";
        merge_first_node = true;
#endif
        base_range = map_it1->first;
        node_range = map_it1->second;
        segs = GetSegmentsFromIndexRange(node1, node_range);
        ++map_it1;
        ++map_it2;
      } else if (base_range1.start_idx + base_range1.num_subsequent
          <= base_range2.start_idx) {
        // base_range1 is entirely preceding the base_range2
#ifdef DEBUG
        merge_first_node = true;
#endif
        base_range = base_range1;
        node_range = node_range1;
        segs = GetSegmentsFromIndexRange(node1, node_range);
        ++map_it1;
      } else if (base_range2.start_idx + base_range2.num_subsequent
                 <= base_range1.start_idx) {
        // base_range2 is entirely preceding the base_range1
#ifdef DEBUG
        merge_first_node = false;
#endif
        base_range = base_range2;
        node_range = node_range2;
        segs = GetSegmentsFromIndexRange(node2, node_range);
        ++map_it2;
      } else {
        LOG(INFO) << "Merging fails.";
        return Hash();
      }  // end if (base_range1.start_idx + base_range1.num_subsequent
    }  // end if (map_it1 == range_map1.end())

#ifdef DEBUG
    std::string msg;
    if (merge_first_node) {
      msg = "Merge Node 1 ";
    } else {
      msg = "Merge Node 2 ";
    }

    size_t num_entries = 0;
    for (const auto& seg : segs) {
      num_entries += seg->numEntries();
    }


    DLOG(INFO) << msg << "with # of Segments: " << segs.size()
               << " # of entries: " << num_entries
               << " Base Range: " << IndexRange::to_str(base_range)
               << " Node Range: " << IndexRange::to_str(node_range)
               << "--------------------------------------------";
#endif
    builder.Splice(base_range.start_idx,
                   base_range.num_subsequent,
                   std::move(segs));
  }  // end while
  Hash result = builder.Commit(chunker);

  return result;
}

using KeyMerger = NodeMerger<OrderedKeyMapper>;
using IndexMerger = NodeMerger<MinEditDistanceMapper>;
}  // namespace ustore

#endif  // USTORE_NODE_NODE_MERGER_H_
