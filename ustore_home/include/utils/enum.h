// Copyright (c) 2017 The Ustore Authors.
#ifndef USTORE_UTILS_ENUM_H_
#define USTORE_UTILS_ENUM_H_

#include <iterator>

#include "utils/type_traits.h"

namespace ustore {

template <typename T, typename = enable_if_t<std::is_enum<T>::value>>
struct Enum {
  class Iterator : public std::iterator<std::input_iterator_tag, T> {
   public:
    explicit Iterator(int pos) : pos_(pos) {}

    Iterator& operator++() {
      ++pos_;
      return *this;
    }

    Iterator& operator++(int) {
      Iterator it = *this;
      ++(*this);
      return it;
    }

    T operator*() const {
      return (T)pos_;
    }

    Iterator(const Iterator&) noexcept = default;
    Iterator& operator=(const Iterator&) noexcept = default;
    bool operator==(const Iterator& other) const { return pos_ == other.pos_; }
    bool operator!=(const Iterator& other) const { return !(*this == other); }

   private:
     int pos_;
  };
};

template <typename T, typename = enable_if_t<std::is_enum<T>::value>>
typename Enum<T>::Iterator begin(Enum<T>) {
  return typename Enum<T>::Iterator(static_cast<int>(T::First));
}

template <typename T, typename = enable_if_t<std::is_enum<T>::value>>
typename Enum<T>::Iterator end(Enum<T>) {
  return typename Enum<T>::Iterator(static_cast<int>(T::Last) + 1);
}

}  // namespace ustore

#endif  // USTORE_UTILS_ENUM_H_
