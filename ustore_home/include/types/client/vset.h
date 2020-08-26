// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VSET_H_
#define USTORE_TYPES_CLIENT_VSET_H_

#include <memory>
#include <vector>
#include "types/client/vobject.h"
#include "types/uset.h"

namespace ustore {

class VSet : public USet, public VObject {
  friend class VMeta;

 public:
  VSet() noexcept : VSet(std::vector<Slice>()) {}
  VSet(VSet&&) = default;
  VSet& operator=(VSet&&) = default;
  // Create new VSet
  explicit VSet(const std::vector<Slice>& keys) noexcept;
  ~VSet() = default;

  Hash Set(const Slice& key) const override;
  Hash Remove(const Slice& key) const override;

 protected:
  // Load an existing VSet
  VSet(std::shared_ptr<ChunkLoader>, const Hash& root_hash) noexcept;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VSET_H_
