// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_BLOB_NODE_H_
#define USTORE_NODE_BLOB_NODE_H_

#include <memory>
#include <vector>

#include "node/node.h"
#include "utils/singleton.h"

namespace ustore {

class BlobChunker : public Singleton<BlobChunker>, public Chunker {
  friend class Singleton<BlobChunker>;

 public:
  ChunkInfo Make(const std::vector<const Segment*>& segments) const override;
  inline bool isFixedEntryLen() const override { return true; }

 private:
  BlobChunker() = default;
  ~BlobChunker() = default;
};

class BlobNode : public LeafNode {
  /*
  BlobNode is a leaf node in Prolly tree that contains
  actual blob data

  Encoding Scheme:
    | ------blob bytes ------|
    | ------variable size
  */
 public:
  explicit BlobNode(const Chunk* chunk) : LeafNode(chunk) {}
  ~BlobNode() = default;

  const byte_t* data(size_t idx) const override { return chunk_->data() + idx; }
  // return the byte len of the idx-th entry
  inline size_t len(size_t idx) const override { return 1; }
  inline size_t numEntries() const override { return chunk_->capacity(); }
  inline size_t GetLength(size_t start, size_t end) const override {
    return end - start;
  }
  OrderedKey key(size_t idx) const override;
  size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const override;
  uint64_t FindIndexForKey(const OrderedKey& key,
                           ChunkLoader* loader) const override;
  std::unique_ptr<const Segment> GetSegment(
      size_t start, size_t num_elements) const override;
};

}  // namespace ustore

#endif  // USTORE_NODE_BLOB_NODE_H_
