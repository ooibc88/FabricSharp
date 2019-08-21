// Copyright (c) 2017 The Ustore Authors.

#include <cstddef>
#include "hash/murmurhash3.h"

#ifndef USTORE_HASH_MURMURHASH_H_
#define USTORE_HASH_MURMURHASH_H_

namespace ustore {

static const uint32_t kMurmurHashSeed = 0xbc9f1d34;  // from LevelDB

inline const uint32_t MurmurHash32(const void* key, const int& len) {
  uint32_t hash;
  MurmurHash3_x86_32(key, len, kMurmurHashSeed, &hash);
  return hash;
}

inline const size_t MurmurHash(const void* key, const int& len) {
  return static_cast<size_t>(MurmurHash32(key, len));
}

}  // namespace ustore

#endif  // USTORE_HASH_MURMURHASH_H_
