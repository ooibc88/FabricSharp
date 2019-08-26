// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_MAP_NODE_H_
#define USTORE_NODE_MAP_NODE_H_

#include <memory>
#include <vector>

#include "chunk/chunk.h"
#include "node/node.h"
#include "utils/singleton.h"

namespace ustore {

struct KVItem {
  Slice key;
  Slice val;
};

class MapChunker : public Singleton<MapChunker>, public Chunker {
  friend class Singleton<MapChunker>;
 public:
  ChunkInfo Make(const std::vector<const Segment*>& segments) const override;
  inline bool isFixedEntryLen() const override { return false; }

 private:
  MapChunker() = default;
  ~MapChunker() = default;
};

class MapNode : public LeafNode {
// Encoding scheme for mapnode
// | # of kv items- | --kv-item1--|--kv-item2--| ---
// 0 -------------- 4 --var size--|--var size--| ---

 public:
  // given the bytes of a kv entry,
  // return ptr to key/value and its size (num_bytes)
  // encodeing scheme for a Key-value item
  // |-num_item_bytes-|-num_key_bytes|--key_bytes-----|--value_bytes--|
  // 0----------------4 -------------8--variable size-|-variable size-|

  // Three decoders for kvitems
  inline static const OrderedKey orderedKey(const byte_t* entry) {
    size_t key_num_bytes;
    const byte_t* key_data = key(entry, &key_num_bytes);
    return OrderedKey(false, key_data, key_num_bytes);
  }

  static const byte_t* key(const byte_t* entry,
                           size_t* key_num_bytes);

  static const byte_t* value(const byte_t* entry,
                             size_t* value_num_bytes);

  static const KVItem kvitem(const byte_t* entry, size_t* item_num_bytes);

  // Encode the kvitem into buffer based on the above scheme
  //   return the number of bytes encoded
  static size_t Encode(byte_t* buffer, const KVItem& item);

  // Encode multiple items into a segment
  static std::unique_ptr<const Segment> Encode(
        const std::vector<KVItem>& items);

  static size_t EncodeNumBytes(const KVItem& kv_item);


  explicit MapNode(const Chunk* chunk) : LeafNode(chunk) {
    PrecomputeOffsets();
  }
  ~MapNode() = default;

  const byte_t* data(size_t idx) const override;
  // return the byte len of the idx-th entry
  size_t len(size_t idx) const override;

  uint64_t FindIndexForKey(const OrderedKey& key,
                           ChunkLoader* loader) const override;

  size_t numEntries() const override;

  size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const override;

  size_t GetLength(size_t start, size_t end) const override;

  OrderedKey key(size_t idx) const override;

  std::unique_ptr<const Segment> GetSegment(
      size_t start, size_t num_elements) const override;

 private:
  void PrecomputeOffsets();
  // a vector byte offset relative to chunk data
  std::vector<size_t> offsets_;
};
}  // namespace ustore

#endif  // USTORE_NODE_MAP_NODE_H_
