// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_ROCKS_STORE_H_
#define USTORE_STORE_ROCKS_STORE_H_

#include <mutex>
#include <string>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"
#include "types/type.h"
#include "utils/logging.h"
#include "utils/rocksdb.h"
#include "utils/singleton.h"

namespace ustore {

class RocksStore
  : public RocksDB, public Singleton<RocksStore>, public ChunkStore {
  friend class Singleton<RocksStore>;

 public:
  ~RocksStore();

  Chunk Get(const Hash& key) override;
  bool Exists(const Hash& key) override;
  bool Put(const Hash& key, const Chunk& chunk) override;
  StoreInfo GetInfo() override;

  StoreIterator begin() const override;
  StoreIterator cbegin() const override;
  StoreIterator end() const override;
  StoreIterator cend() const override;

 private:
  RocksStore();
  explicit RocksStore(const std::string& db_path, const bool persist = true);

  void InitStoreInfo();
  void UpdateStoreInfoForNewChunk(const Chunk& chunk);

  bool persist_;
  StoreInfo store_info_;
  std::mutex mtx_store_info_;
};

}  // namespace ustore

#endif  // USTORE_STORE_ROCKS_STORE_H_
