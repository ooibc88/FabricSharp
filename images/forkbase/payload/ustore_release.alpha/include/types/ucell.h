// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UCELL_H_
#define USTORE_TYPES_UCELL_H_

#include <memory>
#include <utility>
#include "chunk/chunk.h"
#include "chunk/chunk_writer.h"
#include "hash/hash.h"
#include "node/cell_node.h"
#include "spec/slice.h"
#include "types/type.h"
#include "utils/logging.h"
#include "utils/noncopyable.h"

namespace ustore {

// UCell always write to local store, so no chunk loader/writer is needed.
class UCell : private Moveable {
 public:
  // Create the chunk data and dump to storage
  // Return the UCell instance
  static UCell Create(UType type, const Slice& key, const Slice& data,
                      const Slice& ctx, const Hash& preHash1,
                      const Hash& preHash2);
  static UCell Create(UType type, const Slice& key, const Hash& data,
                      const Slice& ctx, const Hash& preHash1,
                      const Hash& preHash2);
  static UCell Load(const Hash& unode_hash);

  UCell() = default;
  UCell(UCell&&) = default;
  UCell& operator=(UCell&&) = default;
  // To be called by Load(), Create() and ClientDb()
  explicit UCell(Chunk&& chunk);
  ~UCell() = default;

  inline bool empty() const { return node_.get() == nullptr; }
  inline UType type() const { return node_->type(); }
  inline bool merged() const { return node_->numPreHash() == 2; }
  inline Hash preHash(bool second = false) const {
    return second ? node_->preHash(1) : node_->preHash(0);
  }
  inline Slice key() const {
    return Slice(node_->key(), node_->keyLength());
  }
  inline Slice data() const {
    return Slice(node_->data(), node_->dataLength());
  }
  inline Slice context() const {
    return Slice(node_->ctx(), node_->ctxLength());
  }
  inline Hash dataHash() const {
    if (type() == UType::kBlob || type() == UType::kList
        || type() == UType::kMap || type() == UType::kSet)
      return Hash(node_->data());
    LOG(WARNING) << "The Ucell does not have data hash";
    return Hash();
  }
  // hash of this ucell
  inline Hash hash() const { return node_->hash(); }
  inline const Chunk& chunk() const { return node_->chunk(); }
  inline const std::shared_ptr<const CellNode>& node() const { return node_; }

 private:
  std::shared_ptr<const CellNode> node_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_UCELL_H_
