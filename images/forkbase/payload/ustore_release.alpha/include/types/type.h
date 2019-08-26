// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_TYPE_H_
#define USTORE_TYPES_TYPE_H_

#include <algorithm>
#include <cstdint>
#include <string>


namespace ustore {

typedef unsigned char byte_t;
typedef uint16_t key_size_t;

/*
 * Supported data types
 */
enum class UType : byte_t {
  kUnknown = 0,
  // Primitive types
  kBool = 1,
  kNum = 2,
  kString = 3,
  kBlob = 4,
  // Structured types
  kList = 5,
  kSet = 6,
  kMap = 7,

  First = kBool,
  Last = kMap
};

/*
 * For internal usage
 * Chunk types in chunk store
 */
enum class ChunkType : byte_t {
  // used in chunk store
  kNull = 0,
  // UCell Chunk
  kCell = 1,
  // Meta SeqNode Chunk
  kMeta = 2,
  // Instances of Leaf SeqNode Chunk
  kBlob = 3,
  kMap = 4,
  kList = 5,
  kSet = 6,

  First = kCell,
  Last = kSet,

  // Indicate the validity
  kInvalid = 99
};

static inline bool IsChunkValid(ChunkType type) noexcept {
  return type == ChunkType::kNull
         || type == ChunkType::kCell
         || type == ChunkType::kMeta
         || type == ChunkType::kBlob
         || type == ChunkType::kMap
         || type == ChunkType::kSet
         || type == ChunkType::kList
         || type == ChunkType::kInvalid;
}

/*
 * Worker error code returned to user
 */
enum class ErrorCode : byte_t {
  // common
  kOK = 0,
  kUnknownOp = 1,
  kIOFault = 2,
  kInvalidPath = 3,
  // key/branch/version
  kKeyNotExists = 10,
  kKeyExists = 11,
  kInvalidRange = 12,
  kBranchExists = 13,
  kBranchNotExists = 14,
  kReferringVersionNotExist = 15,
  kInconsistentKey = 16,
  // type
  kInvalidValue = 20,
  kTypeUnsupported = 21,
  kUCellNotExists = 22,
  kFailedCreateUCell = 23,
  kFailedCreateSBlob = 24,
  kFailedCreateSString = 25,
  kFailedCreateSList = 26,
  kFailedCreateSMap = 27,
  kFailedCreateSSet = 28,
  kFailedModifySBlob = 29,
  kFailedModifySList = 30,
  kFailedModifySMap = 31,
  kFailedModifySSet = 32,
  kIndexOutOfRange = 33,
  // chunk
  kChunkNotExists = 40,
  kFailedCreateChunk = 41,
  kStoreInfoUnavailable = 42,
  // relational
  kTypeMismatch = 50,
  kTableNotExists = 51,
  kEmptyTable = 52,
  kNotEmptyTable = 53,
  kColumnNotExists = 54,
  kRowNotExists = 55,
  kRowExists = 56,
  // cli
  kFailedOpenFile = 60,
  kInvalidCommandArgument = 61,
  kUnknownCommand = 62,
  kInvalidSchema = 63,
  kInconsistentType = 64,
  kInvalidParameter = 65,
  kMapKeyNotExists = 66,
  kMapKeyExists = 66,
  kElementExists = 67,
  kUnexpectedSuccess = 68,
  // blob store
  kDatasetNotExists = 70,
  kDataEntryNotExists = 71
};

}  // namespace ustore

namespace std {

template<>
struct hash<::ustore::UType> {
  inline size_t operator()(const ::ustore::UType& obj) const {
    return static_cast<std::size_t>(obj);
  }
};

template<>
struct hash<::ustore::ChunkType> {
  size_t operator()(const ::ustore::ChunkType& key) const {
    return static_cast<std::size_t>(key);
  }
};

template<>
struct hash<::ustore::ErrorCode> {
  inline size_t operator()(const ::ustore::ErrorCode& obj) const {
    return static_cast<std::size_t>(obj);
  }
};

}  // namespace std

#endif  // USTORE_TYPES_TYPE_H_
