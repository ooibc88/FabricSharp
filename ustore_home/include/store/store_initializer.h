// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_STORE_INITIALIZER_H_
#define USTORE_STORE_STORE_INITIALIZER_H_

#include <boost/filesystem.hpp>
#include <string>
#include "store/chunk_store.h"
#include "utils/env.h"

namespace ustore {

namespace fs = boost::filesystem;

/*
 * StoreInitialier is responsible for init the chunk store singleton
 * in the process. It should be inherited by the first class that interact
 * with chunk store.
 */
class StoreInitializer {
 public:
  virtual ~StoreInitializer() = default;

  inline bool persist() { return persist_; }
  inline const std::string& dataDir() { return data_dir_; }
  inline const std::string& dataFile() { return data_file_; }
  inline const std::string& dataPath() { return data_path_; }

 protected:
  StoreInitializer(size_t seed, bool persist) : persist_(persist) {
    // create data dir
    data_dir_ = Env::Instance()->config().data_dir();
    fs::create_directory(fs::path(data_dir_));
    // set data path
    auto pattern = Env::Instance()->config().data_file_pattern();
    data_file_ = pattern + "_" + std::to_string(seed);
    // init chunk store before using it
    store::InitChunkStore(data_dir_, data_file_, persist_);
    // data path for later use
    data_path_ = data_dir_ + "/" + data_file_;
  }

 private:
  bool persist_;
  std::string data_dir_;
  std::string data_file_;
  std::string data_path_;
};

}  // namespace ustore

#endif  // USTORE_STORE_STORE_INITIALIZER_H_

