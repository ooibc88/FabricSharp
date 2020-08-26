// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_ENV_H_
#define USTORE_UTILS_ENV_H_

#include "proto/config.pb.h"
#include "utils/singleton.h"

namespace ustore {

/*
 * USTORE environment
 *  This is the place place for storing global configuration
 * parameters.
 */

class Env : public Singleton<Env>, private Noncopyable {
  friend class Singleton<Env>;
 public:
  static const char* kDefaultConfigFile;

  const Config& config() const { return config_; }
  Config& m_config() { return config_; }

 private:
  Env();
  ~Env() = default;

  Config config_;
};

}  // namespace ustore
#endif  // USTORE_UTILS_ENV_H_
