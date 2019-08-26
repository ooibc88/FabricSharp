// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_FACTORY_H_
#define USTORE_TYPES_SERVER_FACTORY_H_

#include <memory>
#include <utility>
#include <vector>

#include "cluster/chunk_client_service.h"
#include "cluster/partitioner.h"
#include "types/server/sblob.h"
#include "types/server/slist.h"
#include "types/server/smap.h"
#include "utils/noncopyable.h"

namespace ustore {

class ChunkableTypeFactory : private Noncopyable {
 public:
  // local chunkable types
  ChunkableTypeFactory() : ChunkableTypeFactory(nullptr) {}
  // distributed chunkable types
  explicit ChunkableTypeFactory(const Partitioner* ptt) : ptt_(ptt) {
    if (ptt_) {
      // start chunk client service
      client_svc_.Run();
      cli_.push_back(client_svc_.CreateChunkClient());
      writer_.reset(new PartitionedChunkWriter(ptt, &cli_[0]));
    } else {
      writer_.reset(new LocalChunkWriter());
    }
  }
  ~ChunkableTypeFactory() = default;

  // Load exsiting SObject
  template<typename T>
  T Load(const Hash& root_hash) {
    return T(loader(), writer(), root_hash);
  }

  // Create new SObject
  template<typename T, typename... Args>
  T Create(Args&&... args) {
    return T(loader(), writer(), std::forward<Args>(args)...);
  }

  inline std::shared_ptr<ChunkLoader> loader() {
    if (ptt_)
      return std::make_shared<PartitionedChunkLoader>(ptt_, &cli_[0]);
    else
      return std::make_shared<LocalChunkLoader>();
  }

  inline ChunkWriter* writer() { return writer_.get(); }

 private:
  const Partitioner* const ptt_;
  std::unique_ptr<ChunkWriter> writer_;  // chunk writer is shared
  ChunkClientService client_svc_;
  std::vector<ChunkClient> cli_;
};

}  // namespace ustore
#endif  // USTORE_TYPES_SERVER_FACTORY_H_
