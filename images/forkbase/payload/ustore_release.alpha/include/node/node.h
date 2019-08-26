// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_H_
#define USTORE_NODE_NODE_H_

#include <memory>
#include <vector>

#include "chunk/chunk.h"
#include "chunk/chunker.h"
#include "chunk/chunk_loader.h"
#include "chunk/segment.h"
#include "node/orderedkey.h"

namespace ustore {
class UNode {
 public:
  explicit UNode(const Chunk* chunk) : chunk_(chunk) {}
  inline Hash hash() const { return chunk_->hash(); }

 protected:
  const Chunk* chunk_;
};

class SeqNode : public UNode {
  /* SeqNode represents a general node in POS Tree.

     Its subclass is either be a internal node containing meta-data
     Or it can be a leaf node containing blob chunk data
  */
 public:
  static std::unique_ptr<const SeqNode> CreateFromChunk(const Chunk* chunk);

  explicit SeqNode(const Chunk* chunk) : UNode(chunk) {}
  virtual ~SeqNode() = default;  // NOT delete chunk!!

  // Whether this SeqNode is a leaf
  virtual bool isLeaf() const = 0;
  // Number of entries in this SeqNode
  // If this is MetaNode, return the number of containing MetaEntries
  // If this is a leaf, return the number of containing elements
  virtual size_t numEntries() const = 0;

  // number of elements at leaves rooted at this SeqNode
  virtual uint64_t numElements() const = 0;

  // number of leaves rooted at this SeqNode
  virtual uint32_t numLeaves() const = 0;

  // return the byte pointer for the idx-th entry in this node
  virtual const byte_t* data(size_t idx) const = 0;

  virtual OrderedKey key(size_t idx) const = 0;
  // return the byte len of the idx-th entry
  virtual size_t len(size_t idx) const = 0;

  virtual uint64_t FindIndexForKey(const OrderedKey& key,
                                   ChunkLoader* loader) const = 0;

    // Retrieve a segment from the start-th element to start + num_elements
  virtual std::unique_ptr<const Segment> GetSegment(size_t start,
      size_t num_elements) const = 0;
};

class LeafNode : public SeqNode {
  /* LeafNode is a leaf node in POS tree.
   * It is a abstract leaf node for Blob/List/Set/etc...
  */
 public:
  explicit LeafNode(const Chunk* chunk) : SeqNode(chunk) {}
  ~LeafNode() = default;

  inline uint64_t numElements() const { return numEntries(); }

  inline bool isLeaf() const override { return true; }

  inline uint32_t numLeaves() const override { return 1; }
  // Get #bytes from start-th element (inclusive) to end-th element (exclusive)
  virtual size_t GetLength(size_t start, size_t end) const = 0;
  // Copy num_bytes bytes from start-th element (inclusive)
  // Buffer capacity shall be large enough.
  // return the number of bytes actually read
  virtual size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const = 0;
};

class MetaEntry {
  /* MetaEntry points a child MetaNode/LeafNode

    Encoding Scheme by MetaEntry (variable size)
    |-num_bytes-|-num_leaves-|-num_elements-|-data hash-|--Ordered Key---|
    0-----------4 -----------8--------------16----------36-variable size-|
  */
 public:
  // encode a MetaEntry into formated byte array
  // given relevant parameters.
  // @args [out] num_of_bytes encoded.
  static const byte_t* Encode(uint32_t num_leaves, uint64_t num_elements,
                              const Hash& data_hash, const OrderedKey& key,
                              size_t* encode_num_bytes);

  explicit MetaEntry(const byte_t* data) : data_(data) {}
  ~MetaEntry() = default;

  OrderedKey orderedKey() const;

  // num of bytes in MetaEntry
  size_t numBytes() const;
  // num of leaves rooted at this MetaEntry
  uint32_t numLeaves() const;
  // num of elements at all leaves rooted at this MetaEntry
  uint64_t numElements() const;

  Hash targetHash() const;

 private:
  static constexpr size_t kNumBytesOffset = 0;
  static constexpr size_t kNumLeavesOffset = kNumBytesOffset + sizeof(uint32_t);
  static constexpr size_t kNumElementsOffset =
      kNumLeavesOffset + sizeof(uint32_t);
  static constexpr size_t kHashOffset = kNumElementsOffset + sizeof(uint64_t);
  static constexpr size_t kKeyOffset = kHashOffset + Hash::kByteLength;

  const byte_t* data_;  // MetaEntry is NOT responsible to clear
};

}  // namespace ustore
#endif  // USTORE_NODE_NODE_H_
