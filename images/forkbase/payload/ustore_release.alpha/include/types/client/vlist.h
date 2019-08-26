// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VLIST_H_
#define USTORE_TYPES_CLIENT_VLIST_H_

#include <memory>
#include <vector>
#include "types/client/vobject.h"
#include "types/ulist.h"

namespace ustore {

class VList : public UList, public VObject {
  friend class VMeta;

 public:
  VList() noexcept : VList(std::vector<Slice>()) {}
  VList(VList&&) = default;
  VList& operator=(VList&&) = default;
  // Create new VList
  explicit VList(const std::vector<Slice>& elements) noexcept;
  ~VList() = default;

  // entry vector can be empty
  Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
              const std::vector<Slice>& entries) const override;

 protected:
  // Load an existing VList
  VList(std::shared_ptr<ChunkLoader>, const Hash& root_hash) noexcept;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VLIST_H_
