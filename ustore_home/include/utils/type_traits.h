// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_TYPE_TRAITS_H_
#define USTORE_UTILS_TYPE_TRAITS_H_

#include <type_traits>

namespace ustore {

template <int N>
struct Int2Type{
    enum {value = N};
};

template <typename T>
struct is_char{
    constexpr static bool value = false;
};

template <>
struct is_char<char>{
    constexpr static bool value = true;
};

template <>
struct is_char<signed char>{
    constexpr static bool value = true;
};

template <>
struct is_char<unsigned char>{
    constexpr static bool value = true;
};

template <bool B, typename T = void>
  using enable_if_t = typename std::enable_if<B, T>::type;

template <typename T>
  using remove_reference_t = typename ::std::remove_reference<T>::type;

template <typename T>
  using remove_cv_t = typename ::std::remove_cv<T>::type;

template <typename T>
  using is_integral_t
    = typename ::ustore::enable_if_t<std::is_integral<T>::value, T>;

template <typename T>
  using not_integral_t
    = typename ::ustore::enable_if_t<!std::is_integral<T>::value, T>;

template <typename T>
  using is_char_t
    = typename ::ustore::enable_if_t<::ustore::is_char<T>::value, T>;

template <typename T>
  using not_char_t
    = typename ::ustore::enable_if_t<!::ustore::is_char<T>::value, T>;

}  // namespace ustore

#endif  // USTORE_UTILS_TYPE_TRAITS_H_
