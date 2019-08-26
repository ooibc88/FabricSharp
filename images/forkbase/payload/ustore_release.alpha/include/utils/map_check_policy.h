// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_MAP_CHECK_POLICY_H_
#define USTORE_UTILS_MAP_CHECK_POLICY_H_

#include <utility>
#include "utils/type_traits.h"

namespace ustore {

template <typename MapType>
class NoCheckPolicy {
 protected:
  using key_type = typename MapType::key_type;
  // to avoid the construction of a temporary key_type object
  template <typename... T,
      typename = enable_if_t<std::is_constructible<key_type, T...>::value>>
  constexpr bool check(const MapType&, T&&...) noexcept {
    return true;
  }

  constexpr bool check(const MapType& map, const key_type&) noexcept {
    return true;
  }
};

template <typename MapType>
class CheckExistPolicy {
 protected:
  using key_type = typename MapType::key_type;
  // to avoid the construction of a temporary key_type object
  template <typename... T,
      typename = enable_if_t<std::is_constructible<key_type, T...>::value>>
  constexpr bool check(const MapType& map, T&&... keyArgs) {
    return (map.end() != map.find(key_type{std::forward<T>(keyArgs)...}));
  }

  template <typename T,
           typename = enable_if_t<
             std::is_same<remove_cv_t<remove_reference_t<key_type>>,
                  remove_cv_t<remove_reference_t<T>>>::value>>
  constexpr bool check(const MapType& map, T&& key) {
    return (map.end() != map.find(std::forward<T>(key)));
  }
};

}  // namespace ustore

#endif  // USTORE_UTILS_MAP_CHECK_POLICY_H_
