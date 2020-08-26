// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_ITERATOR_H_
#define USTORE_STORE_ITERATOR_H_

#include <iterator>
#include <memory>
#include <utility>

#include "chunk/chunk.h"

namespace ustore {

class StoreIteratorBase {
 public:
  virtual ~StoreIteratorBase() {}

  bool operator==(const StoreIteratorBase& other) const {
    return typeid(*this) == typeid(other) && equal(other);
  }

  virtual void operator++() = 0;
  virtual Chunk operator*() const = 0;
  // return a newly allocated object
  virtual StoreIteratorBase* clone() const = 0;

 protected:
  virtual bool equal(const StoreIteratorBase& other) const = 0;
};

class StoreIterator : public std::iterator<std::input_iterator_tag,
    Chunk, std::ptrdiff_t, const Chunk*, Chunk> {
 public:
  // take ownership of the pointer
  explicit StoreIterator(StoreIteratorBase* ptr) : itr_(ptr) {}
  StoreIterator(const StoreIterator& other) : itr_(other.itr_->clone()) {}
  StoreIterator(StoreIterator&& other) = default;

  // copy & swap idiom; strong exception guarantee
  // note there is no extra cost
  StoreIterator& operator=(StoreIterator other) {
    std::swap(itr_, other.itr_);
    return *this;
  }

  inline bool operator==(const StoreIterator& other) const {
    return itr_ == other.itr_ || *itr_ == *other.itr_;
  }

  inline bool operator!=(const StoreIterator& other) const {
    return !(*this == other);
  }

  inline StoreIterator& operator++() {
    ++(*itr_);
    return *this;
  }

  inline StoreIterator operator++(int) {
    StoreIterator it = *this;
    ++(*this);
    return it;
  }

  reference operator*() const {
    return itr_->operator*();
  }

 private:
  std::unique_ptr<StoreIteratorBase> itr_;
};

}  // namespace ustore

#endif  // USTORE_STORE_ITERATOR_H_
