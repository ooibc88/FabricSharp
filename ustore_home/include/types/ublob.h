// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UBLOB_H_
#define USTORE_TYPES_UBLOB_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "types/base.h"
#include "types/uiterator.h"

#include "utils/logging.h"

namespace ustore {

class UBlob : public ChunkableType {
 public:
  class Iterator : public CursorIterator {
    /*
    The normal Iterator for UBlob iterator the specified bytes
    in the vector of index ranges
    */
    friend class UBlob;
   public:
    Iterator(Iterator&&) = default;
    Iterator& operator=(Iterator&&) = default;
    ~Iterator() = default;

    inline Slice key() const override {
      LOG(WARNING) << "Key not supported for blob";
      return Slice();
    }

   private:
    // Only used by UBlob
    Iterator(const Hash& root, const std::vector<IndexRange>& ranges,
             ChunkLoader* loader) noexcept
      : CursorIterator(root, ranges, loader) {}

    // Only used by UBlob
    Iterator(const Hash& root, std::vector<IndexRange>&& ranges,
             ChunkLoader* loader) noexcept
      : CursorIterator(root, std::move(ranges), loader) {}
  };

  class ChunkIterator : public UIterator {
    /*
    The Ublob's ChunkIterator iterates one chunk at a time.
    The returned value is a slice
    */
    friend class UBlob;
   public:
    ChunkIterator(ChunkIterator&&) = default;
    ChunkIterator& operator=(ChunkIterator&&) = default;
    ~ChunkIterator() = default;

    bool next() override;
    bool previous() override;

    inline bool head() const override { return cursor_.isBegin(); }
    inline bool end() const override { return cursor_.isEnd(); }

   protected:
    inline Slice RealValue() const override {
      // number of bytes stored in this chunk
      DCHECK_EQ(0, cursor_.idx());
      return Slice(cursor_.current(), NumChunkBytes());
    }

   private:
    // Only used by UBlob
    ChunkIterator(const Hash& root, ChunkLoader* loader) noexcept
      : cursor_(root, 0, loader) {}

    inline size_t NumChunkBytes() const { return cursor_.node()->numEntries(); }

    NodeCursor cursor_;
  };

  // Return the number of bytes in this Blob
  inline size_t size() const { return root_node_->numElements(); }
  /** Read the blob data and copy into buffer
   *    Args:
   *      pos: the number of position to read
   *      len: the number of subsequent bytes to read into buffer
   *      buffer: the byte array which the data is copied to
   *
   *    Return:
   *      the number of bytes that actually read
   */
  size_t Read(size_t pos, size_t len, byte_t* buffer) const;
  /** Read the blob data and copy into std::string buffer
   *    Args:
   *      pos: the number of position to read
   *      len: the number of subsequent bytes to read into buffer
   *      buffer: the string which the data is copied to
   *
   *    Return:
   *      the number of bytes that actually read
   */
  size_t Read(size_t pos, size_t len, std::string* buffer) const;
   /** Delete some bytes from a position and insert new bytes
   *
   *  Args:
   *    pos: the byte position to remove or insert bytes
   *    n_delete_bytes: the number of bytes to be deleted
   *    data: the byte array to insert after deletion
   *    n_insert_bytes: number of bytes in array to be inserted into current blob
   *
   *  Return:
   *    the new Blob reflecting the operation
   */
  virtual Hash Splice(size_t pos, size_t n_delete_bytes, const byte_t* data,
                      size_t n_insert_bytes) const = 0;
  // * Insert bytes given a position
  // * Use Splice internally
  Hash Insert(size_t pos, const byte_t* data, size_t num_insert) const;
  /** Delete bytes from a given position
   *
   *  Use Splice internally
   */
  Hash Delete(size_t pos, size_t num_delete) const;
  /** Append bytes from the last position of Blob
   *
   *  Use Splice internally
   */
  Hash Append(const byte_t* data, size_t num_insert) const;

  inline UBlob::Iterator Scan() const {
    if (numElements() == 0) {
      return Iterator(hash(), {}, chunk_loader_.get());
    } else {
      IndexRange all_range{0, numElements()};
      return Iterator(hash(), {all_range}, chunk_loader_.get());
    }
  }

  inline UBlob::ChunkIterator ScanChunk() const {
    return ChunkIterator(hash(), chunk_loader_.get());
  }

  friend inline std::ostream& operator<<(std::ostream& os, const UBlob& obj) {
    for (auto it = obj.ScanChunk(); !it.end(); it.next()) {
      os << it.value();
    }
    return os;
  }

 protected:
  UBlob() = default;
  UBlob(UBlob&&) = default;
  UBlob& operator=(UBlob&&) = default;
  explicit UBlob(std::shared_ptr<ChunkLoader> loader) noexcept :
    ChunkableType(loader) {}
  ~UBlob() = default;

  bool SetNodeForHash(const Hash& hash) override;
};

}  // namespace ustore
#endif  // USTORE_TYPES_UBLOB_H_
