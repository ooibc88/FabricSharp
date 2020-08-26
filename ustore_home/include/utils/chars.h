// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_CHARS_H_
#define USTORE_UTILS_CHARS_H_

#include <type_traits>

#include "utils/type_traits.h"
#include "utils/logging.h"

static inline size_t AppendInteger(char* buf) { return 0; }
static inline size_t ReadInteger(const char* buf) { return 0; }

template<typename Type1, typename ... Types,
    typename = typename ::ustore::is_integral_t<Type1> >
static size_t AppendInteger(char* buf, Type1 value, Types ... values) {
  // make sure alignment
  CHECK_EQ((uintptr_t)buf % sizeof(Type1), Type1(0));
  *(reinterpret_cast<Type1*>(buf)) = value;
  return sizeof(Type1) + AppendInteger(buf + sizeof(Type1), values...);
}

template<typename Type1, typename ... Types,
    typename = typename ::ustore::is_integral_t<Type1> >
static size_t ReadInteger(const char* buf, Type1& value, Types&... values) {
  // make sure alignment
  CHECK_EQ((uintptr_t)buf % sizeof(Type1), Type1(0));
  value = *reinterpret_cast<Type1*>(const_cast<char*>(buf));
  return sizeof(Type1)
         + ReadInteger(const_cast<char*>(buf) + sizeof(Type1), values...);
}

template<typename T, int N>
static size_t ReadInteger(const char* buf, T* array);

template<typename T, int N>
static size_t ReadInteger(const char* buf, T (&array)[N] ) {
  return ReadInteger<T, N>(buf, reinterpret_cast<T*>(array));
}

template<typename T, int N>
static size_t ReadInteger(const char* buf, T* array, std::true_type) {
  return ReadInteger<T, 1>(buf, array)
         + ReadInteger<T, N-1>(buf + sizeof(T), array + 1);
}

template<typename T, int N>
static size_t ReadInteger(const char* buf, T* array, std::false_type) {
  return ReadInteger(buf, *array);
}

template<typename T, int N>
static size_t ReadInteger(const char* buf, T* array) {
  return ReadInteger<T, N>(buf, array, std::integral_constant<bool, (N>1)>());
}

#endif  // USTORE_UTILS_CHARS_H_
