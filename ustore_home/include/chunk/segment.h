// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_SEGMENT_H_
#define USTORE_CHUNK_SEGMENT_H_

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "types/type.h"

namespace ustore {
class Segment {
  // Segment points to a continuous memory holding multiple entries
 public:
  //   The segment does not own the data using this constructor.
  Segment(const byte_t* data, size_t num_bytes)
      : data_(data), num_bytes_(num_bytes) {}

  //   The segment owns the data using this constructor.
  Segment(std::unique_ptr<const byte_t[]> data, size_t num_bytes)
      : own_(std::move(data)), data_(own_.get()), num_bytes_(num_bytes) {}

  // create an empty segment, to be prolonged later
  //   The segment does not own the data using this constructor.
  explicit Segment(const byte_t* data) : data_(data), num_bytes_(0) {}

  virtual ~Segment() {}

  virtual const byte_t* entry(size_t idx) const = 0;
  // prolong the segment by entries,
  //   for VarSegment can only prolong by one entry,
  //   for FixedSegment can prolong by multiple entries.
  // given its number of bytes
  //   prolong will keep the pointed data unchanged
  // return the number of entries after prolong operation
  virtual size_t prolong(size_t entry_num_bytes_) = 0;
  virtual size_t entryNumBytes(size_t idx) const = 0;
  virtual size_t numEntries() const = 0;
  // Split a segment at idx-entry into two segments
  //   the idx entry is the first of the second segments
  //   if idx = numEntries(), the second segment is empty
  // The splitted segments does not own the data
  virtual std::pair<std::unique_ptr<const Segment>,
                    std::unique_ptr<const Segment>>
  Split(size_t idx) const = 0;
  // Given a position in the raw byte data, find the
  // index of corresponding entry.
  virtual size_t PosToIdx(size_t) const = 0;
  // Append this segment on the chunk buffer
  //   number of appended bytes = numBytes()
  // Current implementation is the same for VarSegment and FixSegment
  virtual void AppendForChunk(byte_t* chunk_buffer) const;

  size_t numBytes() const { return num_bytes_; }
  bool empty() const { return numEntries() == 0; }
  const byte_t* data() const { return data_; }

 protected:
  // own the chunk if created by itself
  std::unique_ptr<const byte_t[]> own_;
  const byte_t* data_;
  size_t num_bytes_;
};

class FixedSegment : public Segment {
  // Entries in FixedSegment are of the same number of bytes
 public:
  FixedSegment(std::unique_ptr<const byte_t[]> data, size_t num_bytes,
               size_t bytes_per_entry)
      : Segment(std::move(data), num_bytes),
        bytes_per_entry_(bytes_per_entry) {}
  FixedSegment(const byte_t* data, size_t num_bytes, size_t bytes_per_entry)
      : Segment(data, num_bytes), bytes_per_entry_(bytes_per_entry) {}
  // create an empty FixedSegment given the data pointer
  FixedSegment(const byte_t* data, size_t bytes_per_entry)
      : Segment(data), bytes_per_entry_(bytes_per_entry) {}
  ~FixedSegment() override {}

  const byte_t* entry(size_t idx) const override;
  size_t prolong(size_t entry_num_bytes) override;
  size_t entryNumBytes(size_t idx) const override;
  inline size_t numEntries() const override {
    return num_bytes_ / bytes_per_entry_;
  }

  size_t PosToIdx(size_t) const override;
  std::pair<std::unique_ptr<const Segment>, std::unique_ptr<const Segment>>
  Split(size_t idx) const override;

 private:
  size_t bytes_per_entry_;
};

class VarSegment : public Segment {
  // Entries in VarSegment are of variable number of bytes
 public:
  VarSegment(std::unique_ptr<const byte_t[]> data, size_t num_bytes,
             std::vector<size_t>&& entry_offsets)
      : Segment(std::move(data), num_bytes),
        entry_offsets_(std::move(entry_offsets)) {}
  VarSegment(const byte_t* data, size_t num_bytes,
             std::vector<size_t>&& entry_offsets)
      : Segment(data, num_bytes), entry_offsets_(std::move(entry_offsets)) {}
  // An empty var segment to be prolonged later
  explicit VarSegment(const byte_t* data) : Segment(data) {}
  ~VarSegment() override {}

  const byte_t* entry(size_t idx) const override;
  size_t prolong(size_t entry_num_bytes_) override;
  size_t entryNumBytes(size_t idx) const override;
  inline size_t numEntries() const override { return entry_offsets_.size(); }

  size_t PosToIdx(size_t) const override;
  std::pair<std::unique_ptr<const Segment>, std::unique_ptr<const Segment>>
  Split(size_t idx) const override;

 private:
  std::vector<size_t> entry_offsets_;
};

}  // namespace ustore
#endif  // USTORE_CHUNK_SEGMENT_H_
