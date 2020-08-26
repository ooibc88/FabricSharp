// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMETA_H_
#define USTORE_TYPES_CLIENT_VMETA_H_

#include <utility>
#include "spec/db.h"
#include "spec/value.h"
#include "types/ucell.h"
#include "types/client/vblob.h"
#include "types/client/vlist.h"
#include "types/client/vmap.h"
#include "types/client/vset.h"
#include "types/client/vstring.h"
#include "utils/noncopyable.h"

namespace ustore {

// TODO(wangsh): modify DB api and remove version in VMeta
class VMeta : private Moveable {
 public:
  VMeta(DB* db, UCell&& cell, std::shared_ptr<ChunkLoader> loader)
    : db_(db), cell_(std::move(cell)), loader_(loader) {}
  VMeta(DB* db, UCell&& cell) : db_(db), cell_(std::move(cell)) {}
  VMeta(VMeta&&) = default;
  VMeta& operator=(VMeta&&) = default;
  ~VMeta() = default;

  inline UType type() const { return cell_.type(); }
  inline const UCell& cell() const { return cell_; }

  VBlob Blob() const;
  VString String() const;
  VList List() const;
  VMap Map() const;
  VSet Set() const;

  friend std::ostream& operator<<(std::ostream& os, const VMeta& obj);

 private:
  DB* db_;
  UCell cell_;
  std::shared_ptr<ChunkLoader> loader_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VMETA_H_
