// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_LST_STORE_H_
#define USTORE_STORE_LST_STORE_H_

#include <cstdint>

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <string>
#include <stdexcept>
#include <unordered_map>

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"
#include "store/iterator.h"
#include "types/type.h"
#include "utils/chars.h"
#include "utils/env.h"
#include "utils/map_check_policy.h"
#include "utils/noncopyable.h"
#include "utils/singleton.h"
#include "utils/shared_lock.h"
#include "utils/timer.h"
#include "utils/type_traits.h"

namespace ustore {
namespace lst_store {

using offset_t = uint64_t;

constexpr offset_t kMetaLogSize = 4096;
constexpr offset_t kSegmentSize = (1<<22);
constexpr offset_t kMetaSegmentSize = 2 * sizeof(offset_t);
constexpr size_t kMaxPendingSyncChunks = 1024;
constexpr uint64_t kMaxSyncTimeoutMilliseconds = 3000;

struct LSTHash {
  const byte_t* hash_;
  LSTHash(const byte_t* ptr) noexcept : hash_(ptr) {}  // NOLINT
  bool operator==(const LSTHash& rhs) const {
    return std::memcmp(hash_, rhs.hash_, Hash::kByteLength) == 0;
  }
  Hash ToHash() const noexcept {
    return Hash(const_cast<byte_t*>(hash_));
  }
};

}  // namespace lst_store
}  // namespace ustore

namespace std {
template<> struct hash<::ustore::lst_store::LSTHash> {
  size_t operator()(const ::ustore::lst_store::LSTHash& key) const {
    size_t ret;
    std::copy(key.hash_, key.hash_ + sizeof(ret),
              reinterpret_cast<::ustore::byte_t*>(&ret));
    return ret;
  }
};
}  // namespace std

namespace ustore {
namespace lst_store {

struct LSTChunk {
  const byte_t* chunk_;
  LSTChunk(const byte_t* ptr) noexcept: chunk_(ptr) {}  // NOLINT
};

/**
 * @brief layout: 8-byte prev offset | 8-byte next offset | [20-byte hash,
 * chunks] | 20-byte hash which is the same as the hash of the first chunk
 * and acts as the crc code
 */
struct LSTSegment {
  static void* base_addr_;
  LSTSegment *prev_, *next_;
  void* segment_;

  inline explicit LSTSegment(void* segment) noexcept : segment_(segment) {}
};

static inline bool IsEndChunk(ChunkType type) {
  return type == ChunkType::kNull;
}

static inline size_t PtrToChunkLength(const byte_t* byte) {
  return *reinterpret_cast<const uint32_t*>(
      byte + Chunk::kNumBytesOffset);
}

static inline byte_t* GetPtrToNextChunk(const byte_t* byte) {
  return const_cast<byte_t*>(byte) + Hash::kByteLength + PtrToChunkLength(byte);
}

static inline byte_t* GetFirstChunkPtr(const LSTSegment* segment) {
  return reinterpret_cast<byte_t*>(const_cast<void*>(segment->segment_))
    + kMetaSegmentSize;
}

static inline ChunkType PtrToChunkType(const byte_t* byte) {
  return *reinterpret_cast<const ChunkType*>(
      byte + Chunk::kChunkTypeOffset);
}

template <typename MapType,
         template<typename> class CheckPolicy = NoCheckPolicy>
class LSTStoreIterator : public StoreIteratorBase,
                         protected CheckPolicy<MapType> {
 public:
  using BaseIterator = LSTStoreIterator;

  LSTStoreIterator(const MapType& map,
      const LSTSegment* first,
      const byte_t* ptr) noexcept : map_(map), segment_(first), ptr_(ptr) {}

  LSTStoreIterator(const LSTStoreIterator&) = default;
  LSTStoreIterator& operator=(const LSTStoreIterator&) = default;

  bool operator==(const LSTStoreIterator& other) const {
    return ptr_ == other.ptr_;
  }

  void operator++() override {
    if (ptr_ == nullptr || IsEndChunk(PtrToChunkType(ptr_))) {
      throw std::out_of_range("LSTStoreIterator");
    }

    do {
      ptr_ = GetPtrToNextChunk(ptr_);
      if (IsEndChunk(PtrToChunkType(ptr_)) && segment_->next_ != nullptr) {
        segment_ = segment_->next_;
        ptr_ = GetFirstChunkPtr(segment_);
      }
    } while (!CheckPolicy<MapType>::check(map_, ptr_ + PtrToChunkLength(ptr_))
             && !IsEndChunk(PtrToChunkType(ptr_)));

    DCHECK(IsChunkValid(PtrToChunkType(ptr_)));
  }

  Chunk operator*() const override {
    DCHECK(CheckPolicy<MapType>::check(map_, ptr_ + PtrToChunkLength(ptr_)));
    return Chunk(ptr_, ptr_ + PtrToChunkLength(ptr_));
  }

  LSTStoreIterator* clone() const override {
    return new LSTStoreIterator(*this);
  }

 protected:
  const MapType& map_;
  const LSTSegment* segment_;
  const byte_t* ptr_;

  bool equal(const StoreIteratorBase& other) const override {
    return operator==(
        static_cast<const LSTStoreIterator<MapType, CheckPolicy>&>(other));
  }
};

template <typename MapType, typename ChunkType, ChunkType T,
         template<typename> class CheckPolicy = NoCheckPolicy>
class LSTStoreTypeIterator : public LSTStoreIterator<MapType, CheckPolicy> {
 public:
  using parent = LSTStoreIterator<MapType, CheckPolicy>;
  using BaseIterator = parent;

  static constexpr ChunkType type_ = T;

  LSTStoreTypeIterator(const LSTStoreTypeIterator&) noexcept = default;
  LSTStoreTypeIterator& operator=(const LSTStoreTypeIterator&) = default;

  LSTStoreTypeIterator(const MapType& map,
      const LSTSegment* segment,
      const byte_t* ptr) : parent(map, segment, ptr) {
    if (!parent::ptr_) return;
    ChunkType type = PtrToChunkType(parent::ptr_);
    if (type != type_ && !IsEndChunk(type))
      operator++();
  }

  void operator++() override {
    ChunkType type;
    do {
      parent::operator++();
      type = PtrToChunkType(parent::ptr_);
    } while (type != type_ && !IsEndChunk(type));
  }

  LSTStoreTypeIterator* clone() const override {
    return new LSTStoreTypeIterator(*this);
  }
};

class LSTStore : public ChunkStore
                 , private Noncopyable
                 , public Singleton<LSTStore, ClassLevelLockable>
                 , public ObjectLevelLockable<LSTStore> {
  friend class Singleton<LSTStore, ClassLevelLockable>;
  using Lock = typename ObjectLevelLockable<LSTStore>::Lock;

 public:
  using MapType = std::unordered_map<LSTHash, LSTChunk>;

  // normal iterators
  using iterator = LSTStoreIterator<MapType, CheckExistPolicy>;
  using const_iterator = LSTStoreIterator<MapType, CheckExistPolicy>;
  using unsafe_iterator = LSTStoreIterator<MapType, NoCheckPolicy>;
  using unsafe_const_iterator = LSTStoreIterator<MapType, NoCheckPolicy>;

  // type iterators
  template <typename ChunkType, ChunkType T>
    using type_iterator = LSTStoreTypeIterator<MapType, ChunkType, T,
                                               CheckExistPolicy>;
  template <typename ChunkType, ChunkType T>
    using const_type_iterator = LSTStoreTypeIterator<MapType, ChunkType, T,
                                                     CheckExistPolicy>;
  template <typename ChunkType, ChunkType T>
    using unsafe_type_iterator = LSTStoreTypeIterator<MapType, ChunkType, T,
                                                      NoCheckPolicy>;
  template <typename ChunkType, ChunkType T>
    using unsafe_const_type_iterator = LSTStoreTypeIterator<MapType, ChunkType,
                                                            T, NoCheckPolicy>;

  void Sync() const;
  Chunk Get(const Hash& key) override;
  bool Exists(const Hash& key) override {
    shared_lock<shared_mutex> lock(chunk_map_mutex_);
    return chunk_map_.find(key.value()) != chunk_map_.end();
  }
  bool Put(const Hash& key, const Chunk& chunk) override;
  StoreInfo GetInfo() override {
    return storeInfo;
  }

  StoreIterator begin() const override { return begin<iterator>(); }
  StoreIterator cbegin() const override { return begin(); }
  StoreIterator end() const override { return end<iterator>(); }
  StoreIterator cend() const override { return cend(); }

  template <typename Iterator = iterator>
  StoreIterator begin() const {
    const byte_t* ptr = major_list_ ? GetFirstChunkPtr(major_list_) : nullptr;
    return StoreIterator(new Iterator(chunk_map_, major_list_, ptr));
  }

  template <typename Iterator = iterator>
  StoreIterator cbegin() const {
    return begin<Iterator>();
  }

  template <typename Iterator = iterator>
  StoreIterator end() const {
    const byte_t* ptr = current_major_segment_ ?
      (const byte_t*)current_major_segment_->segment_ + major_segment_offset_
      : nullptr;
    return StoreIterator(new Iterator(chunk_map_, current_major_segment_, ptr));
  }

  template <typename Iterator = iterator>
  StoreIterator cend() const {
    return end<Iterator>();
  }

 private:
  // TODO(qingchao): add a remove-old-log flag to ease unit test
  struct SyncBlock{
    LSTSegment *head_, *last_;
    int segments_;
  };

  enum class ThreadStatus: byte_t {
    kUnscheduled,
    kScheduled,
    kCompleted
  };

  LSTStore() : LSTStore(".", "ustore_default", false) {}
  LSTStore(const std::string& dir, const std::string& file, bool persist)
    : max_segments_(Env::Instance()->config().max_segments())
      , max_log_size_(kSegmentSize * max_segments_ + kMetaLogSize)
      , thread_status_(ThreadStatus::kUnscheduled)
      , sync_timer_(TimerPool::GetTimer("Sync Store"))
      , write_timer_(TimerPool::GetTimer("Write Chunk")) {
    MmapUstoreLogFile(dir, file, persist);
  }
  ~LSTStore() noexcept(false);

  inline offset_t GetFreeSpaceMajor() {
    return kSegmentSize - major_segment_offset_ -
      Hash::kByteLength - Chunk::kMetaLength;
  }

  void GC() {}
  void Load(void*);
  offset_t LoadFromValidSegment(LSTSegment*);
  offset_t LoadFromLastSegment(LSTSegment*);
  void* MmapUstoreLogFile(const std::string& dir, const std::string& file,
                          bool persist);
  LSTSegment* Allocate(LSTSegment*);
  LSTSegment* AllocateMajor();
  LSTSegment* AllocateMinor();
  /**
   * @brief enlarge the log
   */
  void Enlarge();
  void UpdateStore();

  MapType chunk_map_;
  LSTSegment *free_list_, *major_list_;
  LSTSegment *current_major_segment_;
  LSTSegment *last_free_segment_;

  offset_t major_segment_offset_;
  const size_t max_segments_ = 64;
  const offset_t max_log_size_ = kSegmentSize * max_segments_ + kMetaLogSize;
  const offset_t segment_increment_ = 16;  // # of segments to allocate

  int fd_;  // mmaped file descriptor

  StoreInfo storeInfo;
  SyncBlock sync_block_;
  std::future<void> async_future_;
  std::atomic<ThreadStatus> thread_status_;

  // for thread safety
  shared_mutex chunk_map_mutex_;

  // for persistence
  Timer& sync_timer_;
  Timer& write_timer_;
  size_t to_sync_chunks_ = 0;
};

}  // namespace lst_store
}  // namespace ustore

#endif  // USTORE_STORE_LST_STORE_H_
