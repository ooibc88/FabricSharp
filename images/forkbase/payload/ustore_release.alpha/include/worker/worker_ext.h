// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_EXT_H_
#define USTORE_WORKER_WORKER_EXT_H_

#include <vector>
#include "worker/worker.h"

namespace ustore {

class WorkerExt : public Worker {
 public:
  explicit WorkerExt(const WorkerID& id, const Partitioner* ptt, bool persist)
    : Worker(id, ptt, persist) {}
  ~WorkerExt() = default;

  ErrorCode GetForType(const UType& type, const Slice& key,
                       const Hash& ver, UCell* ucell);

  inline ErrorCode GetForType(const UType& type, const Slice& key,
                              const Slice& branch, UCell* ucell) {
    Hash head;
    head_ver_.GetBranch(key, branch, &head);
    return GetForType(type, key, head, ucell);
  }
};

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_EXT_H_
