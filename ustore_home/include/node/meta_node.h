// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_META_NODE_H_
#define USTORE_NODE_META_NODE_H_

#include <memory>
#include <vector>

#include "node/node.h"
#include "types/type.h"
#include "utils/singleton.h"

namespace ustore {

class MetaChunker : public Singleton<MetaChunker>, public Chunker {
  friend class Singleton<MetaChunker>;

 public:
  ChunkInfo Make(const std::vector<const Segment*>& segments) const override;
  inline bool isFixedEntryLen() const override { return false; }

 private:
  MetaChunker() = default;
  ~MetaChunker() = default;
};

class MetaNode : public SeqNode {
  /* MetaNode is a non-leaf node in prolly tree.

    It consists of multiple MetaEntries.

    Encoding Scheme by MetaNode:
    |-num_entries-|-----MetaEntry 1----|-----MetaEntry 2----|
    0------------ 4 ----------variable size
  */
 public:
  explicit MetaNode(const Chunk* chunk) : SeqNode(chunk) { PrecomputeOffset(); }
  ~MetaNode() = default;

  inline bool isLeaf() const override { return false; }
  size_t numEntries() const override;
  uint64_t numElements() const override;
  uint32_t numLeaves() const override;

  // the total number of elements from the first
  // to the entryidx-th entry (exclusive)
  // Caller of this method has to make sure entry_idx is valid.
  uint64_t numElementsUntilEntry(size_t entry_idx) const;

  // return the byte pointer for the idx-th entry in this node
  const byte_t* data(size_t idx) const override;

  OrderedKey key(size_t idx) const override;

  // return the byte len of the idx-th entry
  size_t len(size_t idx) const override;

  uint64_t FindIndexForKey(const OrderedKey& key,
                           ChunkLoader* loader) const override;

  // Retreive the ChildHash in the MetaEntry
  // which contains the idx-th element rooted at this metanode
  // Return empty hash and entry_idx=numEntries if such MetaEntry not exist.
  Hash GetChildHashByIndex(size_t element_idx, size_t* entry_idx) const;

  // Retreive the ChildHash in the entry_idx-th MetaEntry
  // Caller of this method has to make sure entry_idx is valid.
  Hash GetChildHashByEntry(size_t entry_idx) const;

  // Retreive the child hash pointed by the MetaEntry,
  // The Ordered Key of this MetaEntry
  // has the smallest OrderedKey that is no smaller than the compared key
  // Return empty hash and entry_idx=numEntries if such MetaEntry not exist.
  Hash GetChildHashByKey(const OrderedKey& key, size_t* entry_idx) const;

  std::unique_ptr<const Segment> GetSegment(size_t start,
      size_t num_elements) const override;

 private:
  size_t entryOffset(size_t idx) const;

  // compute the offset for all entries and store the offsets
  //   in a class member vector
  void PrecomputeOffset();

  std::vector<size_t> offsets_;
};

}  // namespace ustore
#endif  // USTORE_NODE_META_NODE_H_
