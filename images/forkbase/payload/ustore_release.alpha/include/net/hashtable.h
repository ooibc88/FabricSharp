// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_NET_HASHTABLE_H_
#define USTORE_USTORE_NET_HASHTABLE_H_

#include <assert.h>
#include <mutex>
#include <array>
#include <string>
#include <exception>
#include <unordered_map>

// TODO(zhanghao): need to clean these code, we do not have this flag
// TODO(zhanghao): why not use std lib for hashtable ?
#ifdef USE_COCKOOHASH

#include "../lib/libcuckoo/src/cuckoohash_map.hh"
#include "../lib/libcuckoo/src/city_hasher.hh"
#include "net/murmur_hasher.h"


const size_t lock_array_size = 1 << 16;

template<typename Key, typename T>
#ifdef USE_CITYHASH
class HashTable : public cuckoohash_map<Key, T, CityHasher<Key>> {
#elif defined(USE_MURMURHASH)
  class HashTable: public cuckoohash_map<Key, T, MurmurHasher<Key>> {
#else
    class HashTable: public cuckoohash_map<Key, T> {
#endif

 public:
  HashTable(std::string name = "DEFAULT_HASHTABLE_NAME",
            size_t n = DEFAULT_SIZE, double mlf = DEFAULT_MINIMUM_LOAD_FACTOR,
            size_t mhp = NO_MAXIMUM_HASHPOWER)
      : name(name),
#ifdef USE_CITYHASH
        cuckoohash_map<Key, T, CityHasher<Key>>::cuckoohash_map(n, mlf, mhp)
#elif defined(USE_MURMURHASH)
  cuckoohash_map<Key, T, MurmurHasher<Key>>::cuckoohash_map(n, mlf, mhp)
#else
  cuckoohash_map<Key, T>::cuckoohash_map(n, mlf, mhp)
#endif
  {
  }
  void lock(Key key);
  void unlock(Key key);
  bool try_lock(Key key);
  inline size_t count(Key key) {
    return this->contains(key);
  }
  // overwrite the default at that returns a reference
  inline T at(const Key& key) {
    T ret;
    try {
      ret = this->find(key);
    } catch (const std::exception& e) {
      printf("cannot find the key for hash table %s (%s)", name.c_str(),
             e.what());
      assert(false);
    }
    return ret;
  }

 private:
  std::array<LockWrapper, lock_array_size> lock_;
  std::string name;
};

template<typename Key, typename T>
inline void HashTable<Key, T>::lock(Key key) {
#ifdef USE_CITYHASH
  lock_[cuckoohash_map<Key, T, CityHasher<Key>>::hash_function()(key)
      % lock_array_size].lock();
#elif defined(USE_MURMURHASH)
  lock_[cuckoohash_map<Key, T, MurmurHasher<Key>>::hash_function()(key)
        % lock_array_size].lock();
#else
  lock_[cuckoohash_map<Key, T>::hash_function()(key) % lock_array_size].lock();
#endif
}

template<typename Key, typename T>
inline bool HashTable<Key, T>::try_lock(Key key) {
#ifdef USE_CITYHASH
  return lock_[cuckoohash_map<Key, T, CityHasher<Key>>::hash_function()(key)
      % lock_array_size].try_lock();
#elif defined(USE_MURMURHASH)
  return lock_[cuckoohash_map<Key, T, MurmurHasher<Key>>::hash_function()(key)
               % lock_array_size].try_lock();
#else
  return lock_[cuckoohash_map<Key, T>::hash_function()(key)
               % lock_array_size].try_lock();
#endif
}

template<typename Key, typename T>
inline void HashTable<Key, T>::unlock(Key key) {
#ifdef USE_CITYHASH
  lock_[cuckoohash_map<Key, T, CityHasher<Key>>::hash_function()(key)
      % lock_array_size].unlock();
#elif defined(USE_MURMURHASH)
  lock_[cuckoohash_map<Key, T, MurmurHasher<Key>>::hash_function()(key)
        % lock_array_size].unlock();
#else
  lock_[cuckoohash_map<Key, T>::hash_function()(key)
        % lock_array_size].unlock();
#endif
}

#else

template<typename Key, typename T>
class HashTable {
 public:
  explicit HashTable(const std::string& name = "DEFAULT_HASHTABLE_NAME") {
    this->name = name;
  }

  inline void lock(const Key& k) {
    lock_.unlock();
  }

  inline void unlock(const Key& k) {
    lock_.unlock();
  }

  inline T& at(const Key& k) {
    lock(k);
    T& v = map_.at(k);
    unlock(k);
    return v;
  }

  inline T& operator[](const Key& k) {
    lock(k);
    T& v = map_[k];
    unlock(k);
    return v;
  }

  inline size_t count(const Key& k) {
    lock(k);
    size_t c = map_.count(k);
    unlock(k);
    return c;
  }

  inline size_t erase(const Key& k) {
    lock(k);
    size_t c = map_.erase(k);
    unlock(k);
    return c;
  }

 private:
  std::mutex lock_;
  std::string name;
  std::unordered_map<Key, T> map_;
};

#endif

#endif  // USTORE_USTORE_NET_HASHTABLE_H_

