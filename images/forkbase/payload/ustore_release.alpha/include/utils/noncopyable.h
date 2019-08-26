// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_NONCOPYABLE_H_
#define USTORE_UTILS_NONCOPYABLE_H_

namespace ustore {

struct Noncopyable {
  Noncopyable() = default;
  ~Noncopyable() = default;
  Noncopyable(const Noncopyable&) = delete;
  Noncopyable& operator=(const Noncopyable&) = delete;
};

struct Moveable : private Noncopyable {
  Moveable() = default;
  ~Moveable() = default;
  Moveable(Moveable&&) {}
  Moveable& operator=(Moveable&&) { return *this; }
};

}  // namespace ustore
#endif  // USTORE_UTILS_NONCOPYABLE_H_
