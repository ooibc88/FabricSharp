// Copyright (c) 2017 The Ustore Authors.
#include "db.h"

#include <iostream>
#include <sstream>

#include "types/client/vmap.h"
#include "store/chunk_store.h"
#include "utils/utils.h"
#include "utils/timer.h"


namespace ustore_kvdb {
using namespace ustore;

const Slice KVDB::DEFAULT_BRANCH("default", 7);
const Slice KVDB::GLOBAL_STATE_KEY("Global", 6);
const size_t kMaxSize = 1<<14;  //16KB


ull power(ull base, size_t exp) {
  ull s = 1;
  for (size_t i = 0; i < exp; ++i) {
    s *= base;
  }
  DLOG(INFO) << " Power for base " << base << " exp " << exp << " result: " << s;
  return s;
}

void KVDB::OutputChunkStorage() {
    auto store_info = store::GetChunkStore()->GetInfo();
    size_t total = 0;
    for (const auto& type_size : store_info.bytesPerType) {
      total += type_size.second;
    }

    std::cout << "Chunk Storages: " << std::endl;
    std::cout << "=========================================" << std::endl;
    std::cout << "Blob: " << store_info.bytesPerType[ChunkType::kBlob] << std::endl;
    std::cout << "Map: " << store_info.bytesPerType[ChunkType::kMap] << std::endl;
    std::cout << "Meta: " << store_info.bytesPerType[ChunkType::kMeta] << std::endl;
    std::cout << "Cell: " << store_info.bytesPerType[ChunkType::kCell] << std::endl;
    std::cout << "Total: " << total << std::endl;
    std::cout << "=========================================" << std::endl;
}

bool NotExists(ErrorCode code) {
  return code == ErrorCode::kUCellNotExists ||
      code == ErrorCode::kKeyNotExists ||
      code == ErrorCode::kBranchNotExists;
}

// Initialize empty map
Status KVDB::InitGlobalState() {
  ustore::VMap global;
  this->base_ = 2;
  auto result = odb_.Put(GLOBAL_STATE_KEY, global, DEFAULT_BRANCH);
  if (result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to init empty global state " << " with error code " << result.stat;
  }
  auto get_result = odb_.Get(GLOBAL_STATE_KEY, DEFAULT_BRANCH);
  if (get_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to fetch the initial global state " << " with error code " << get_result.stat;
  }
  this->global_ = std::move(get_result.value.Map());
  return Status::OK();
}


std::pair<Status, std::string> KVDB::GetBlock(const std::string& key, const std::string& vid) {
  Hash vid_hash = Utils::ToHash(Slice(vid));
  auto ret = odb_.Get(Slice(key), vid_hash);
  if (ret.stat != ErrorCode::kOK) {
    return std::make_pair(Status::NotFound(key), "");
  }

  std::string value;
  if (ret.value.type() == UType::kBlob) {
    auto blob = ret.value.Blob();
    blob.Read(0, blob.size(), &value);
  } else {
    auto str = ret.value.String();
    value.clear();
    value.reserve(str.len());
    value.append(reinterpret_cast<const char*>(str.data()), str.len());
  }
  return std::make_pair(Status::OK(), value);
}

std::pair<Status, std::string> KVDB::PutBlock(const std::string& key, const std::string& value) {
  if (value.length() < kMaxSize) {
    VString val{Slice{value}};
    auto ret = odb_.Put(Slice(key), val, DEFAULT_BRANCH);
    std::string hash_str = Utils::ToSlice(ret.value).ToString();
    return std::make_pair(Status::OK(), hash_str);
  } else {
    VBlob val{Slice{value}};
    auto ret = odb_.Put(Slice(key), val, DEFAULT_BRANCH);
    CHECK(ErrorCode::kOK == ret.stat);
    std::string hash_str = Utils::ToSlice(ret.value).ToString();
    return std::make_pair(Status::OK(), hash_str);
  }
}

// Iterate all the state vids for the given key
void KVDB::IterateState(const std::string& key) {
  auto ret = odb_.Get(Slice(key), DEFAULT_BRANCH);
  CHECK(ret.value.type() == UType::kString);
  std::string value;
  while (ret.stat == ErrorCode::kOK) {
    auto str = ret.value.String();
    value.clear();
    value.reserve(str.len());
    value.append(reinterpret_cast<const char*>(str.data()), str.len());

    Hash pre_hash = ret.value.cell().preHash();
    if (pre_hash == Hash::kNull) {break; }
    ret = odb_.Get(Slice(key), pre_hash);
  }
}

// A utility function that splits a string given the delimiter
std::vector<std::string> split (std::string s, std::string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    std::string token;
    std::vector<std::string> res;

    while ((pos_end = s.find (delimiter, pos_start)) != std::string::npos) {
        token = s.substr (pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back (token);
    }

    res.push_back (s.substr (pos_start));
    return res;
}

Status KVDB::Get(const std::string& key, std::string* value) {
  //auto ret = odb_.Get(Slice(key), DEFAULT_BRANCH);
  auto ret = odb_.Get(Slice(key),DEFAULT_BRANCH);
  if (ret.stat == ErrorCode::kUCellNotExists ||
      ret.stat == ErrorCode::kKeyNotExists ||
      ret.stat == ErrorCode::kBranchNotExists) {
    return Status::NotFound(key);
  }
  CHECK(ErrorCode::kOK == ret.stat);
  if (ret.value.type() == UType::kBlob) {
    auto blob = ret.value.Blob();
    blob.Read(0, blob.size(), value);
  } else {
    auto str = ret.value.String();
    value->clear();
    value->reserve(str.len());
   value->append(reinterpret_cast<const char*>(str.data()), str.len());
  }
  return Status::OK();
}


std::pair<Status, std::string> KVDB::Get(const std::string& key) {
  std::string str;
  auto ret = Get(key, &str);

  return std::make_pair(ret, str);
}

Status KVDB::Put(const std::string& key, const std::string& value) {
  if (value.length() > kMaxSize) {
    VBlob val{Slice{value}};
    auto ret = odb_.Put(Slice{key}, val, DEFAULT_BRANCH);
    CHECK(ErrorCode::kOK == ret.stat);
  } else {
    VString val{Slice{value}};
    auto ret = odb_.Put(Slice{key}, val, DEFAULT_BRANCH);
    CHECK(ErrorCode::kOK == ret.stat);
  }
  return Status::OK();
}

Slice KVDB::GetDepVersionSlice(const std::string& key, const std::string& snapshotVersion) {
  auto fetch_result = odb_.Get(GLOBAL_STATE_KEY, Hash::FromBase32(snapshotVersion));
  if (fetch_result.stat != ErrorCode::kOK) {
    LOG(FATAL) << "Fail to fetch for the state snapshot with version " << snapshotVersion;
  } else {
    DLOG(INFO) << "Successfully retrieve the state snapshot with version " << snapshotVersion;
  }
  auto stateMap = std::move(fetch_result.value.Map());
  ustore::Slice key_slice(key);
  Slice val_slice = stateMap.Get(key_slice);

  return val_slice;
}

bool KVDB::PutState(const std::string& key, const std::string& value,
                    const std::string& txnID,
                    ull blk_idx, const std::vector<std::string>& deps, 
                    const std::string& snapshotVersion) {
#ifdef DEBUG
  Timer t0;
  t0.Start();
  Timer debugTimer;
  debugTimer.Start();
    DLOG(INFO) << "=====================================================================";
    std::string dep_str;
    for (const auto& dep : deps) {
      dep_str += dep + " ";
    }
    DLOG(INFO) << "PutState For Key: " << key << " value: " << value
               << " at blk " << blk_idx << " txnID " << txnID << " with depIDs " << dep_str;
  debugTimer.Stop();
  Timer t1, t2, t3;
  t1.Start();
  t1.Stop();
  // Add on txns and dependency
  Timer depTimer;
  depTimer.Start();
#endif
  if (snapshotVersion == "" && deps.size() > 0) {
    LOG(FATAL) << "Empty snapshot version with non-empty dependent keys...";
  } 
  // Forbid duplicated updated keys in a single block
  if (this->updated_vids_.find(key) != this->updated_vids_.end()) {
    LOG(FATAL) << "Duplicated updates for key " << key;
  }

  // Retrieve vids from the global map for each dependent key
  std::vector<std::string> dep_keys;
  std::vector<std::string> dep_vids;
  int self_dependent = 0;
  for (const std::string& dep_key : deps) {
    ustore::Slice dep_slice(dep_key);
    if (dep_key == key) {
      if (snapshotVersion == "NA") {
        self_dependent = 1;
      } else {
        // Check whether the vid from the latest state and snapshot state is identical. 
        Slice val_slice;
        val_slice = this->GetDepVersionSlice(dep_key, snapshotVersion);
        if (val_slice.empty()) {
          LOG(WARNING) << "Fail to find vid for dep key " << key << " in snapshot " << snapshotVersion;
        } else {
          Slice latest_slice = global_.Get(dep_slice); 
          if (latest_slice.empty()) {
            LOG(WARNING) << "Fail to find vid for dep key " << key << " in  latest state";
          } else if (val_slice == latest_slice) {
            self_dependent = 1;
          }
        }
      }
    } 
    
    Slice val_slice;
    if (snapshotVersion == "NA") {
      // retrieve the dependent version from the latest state by default. 
      val_slice = global_.Get(dep_slice);
    } else {
      val_slice = this->GetDepVersionSlice(dep_key, snapshotVersion);
    }
    if (val_slice.empty()) {
      LOG(WARNING) << "Fail to find vid for dep key " << dep_key << " in snapshot " << snapshotVersion;
    } else {
      dep_keys.push_back(dep_key);
      dep_vids.push_back(val_slice.ToString());
    }
  }  // end for
  
#ifdef DEBUG
  depTimer.Stop();

  Timer daslTimer;
  daslTimer.Start();
#endif

  // if using DASL, 
  //   populate pre_blks and pre_vids with the blk_num and vid of the proceding entry in each level of dasl
  std::vector<ull> pre_blks;
  std::vector<std::string> pre_vids;
#ifdef GO_USE_DASL
  if (!DASLAppend(key, blk_idx, &pre_blks, &pre_vids)) {
    LOG(FATAL) << "DASL Append fails for key " << key << " with blk_idx " << blk_idx;
  }
#endif

#ifdef DEBUG
  daslTimer.Stop();
  debugTimer.Start();
  debugTimer.Stop();
  t2.Start();
#endif

  std::vector<std::string> cur_forward_keys;
  std::vector<std::string> cur_forward_vids;

  // Retrieve the state key and vid which depends on the preceding entry of the to-be-updated state
  // They are associated in Forkbase with key+'-prov' 
  //   in the format of " <depKey1> <depVid1> <depKey2> <depVid2> "
  std::string prov_key = key + "-prov";
  if (odb_.Exists(Slice(prov_key)).value) {
    auto forward_result = odb_.Get(Slice(prov_key), DEFAULT_BRANCH);
    if (forward_result.stat == ErrorCode::kOK) {
      CHECK(forward_result.value.type() == UType::kString);
      auto str = forward_result.value.String();
      std::string prev_forward_info;
      prev_forward_info.clear();
      prev_forward_info.reserve(str.len());
      prev_forward_info.append(reinterpret_cast<const char*>(str.data()), str.len());

      auto splits = split(prev_forward_info, " ");
      CHECK_EQ("", splits[0]);
      DLOG(INFO) << "Size of splits: " << splits.size();
      CHECK_EQ(1, splits.size() % 2);

      for (size_t i = 1; i <= splits.size() / 2; i++) {
        std::string forward_key = splits[2*i-1];
        std::string forward_vid = splits[2*i];
        DLOG(INFO) << "  Forward key " << forward_key << " vid " << forward_vid;
        cur_forward_keys.push_back(forward_key);
        cur_forward_vids.push_back(forward_vid);
      }
      DLOG(INFO) << "Finish update forward key vids...";
    } else {
      LOG(FATAL) << "Fail to retrieve prov key " << prov_key << " with error code " << forward_result.stat;
    }
  }

  std::string encoded = MarshalProv(blk_idx, txnID, cur_forward_keys, 
                                    cur_forward_vids,
                                    dep_keys, dep_vids, 
                                    pre_blks, pre_vids, self_dependent);
#ifdef DEBUG
  t2.Stop();

  t3.Start();
#endif

// Dump the updated value as string and encoded provenance info as context in ForkBase
  VString val{Slice(value)};
  val.SetContext(Slice(encoded));
  auto ret = odb_.Put(Slice(key), val, DEFAULT_BRANCH);
  if (ErrorCode::kOK != ret.stat) {
      LOG(FATAL) << "Fail to write key " << key << " at blk idx " << blk_idx << " with value: " << value;
  } else {
    DLOG(INFO) << "Successfully write key " << key << " at blk idx " << blk_idx << " with coantext: " << encoded;
  }

#ifdef DEBUG
  t3.Stop();
#endif
  std::string vid = ret.value.ToBase32(); 
  // Associate the current key and vid with each of its dependent key
  for (size_t i = 0; i < dep_keys.size(); i++) {
    Slice dep_key(dep_keys[i]);
    Hash dep_version = Hash::FromBase32(dep_vids[i]);
    if (odb_.IsLatestVersion(dep_key, dep_version).value) {
      this->temp_forward_key_vids_[dep_keys[i]] += " " + key + " " + vid;
    } else {
      // It is possible that a dependent state is in stale version in sharp scheduler,
      //  as the sharp scheduler allows transactions with antiRW transaction conflicts. 
      // This is problematic: when we handle with forward dependency, we assume the overrided state
      // will not be referenced or dependent any more. (Refer to Sec 4.3 in http://www.vldb.org/pvldb/vol12/p975-ruan.pdf for details. )

      // Since this assumption breaks under the sharp scheduler, 
      // we temporally exclude any staled dependency for the storage of forward dependency. 
      // Hence the forward dependency query may miss some entries. 
      // Unfortunately, we haven't found an approach to overcome it. 
    }
  }
  // for (const std::string& dep_key : deps) {
  //   this->temp_forward_key_vids_[dep_key] += " " + key + " " + vid;
  // }
  
  this->updated_vids_[key] = vid;
             
#ifdef DEBUG
    DLOG(INFO) << "  Updated vid: " << vid << " for key " << key << " at blk idx " << blk_idx << " with value: " << value;
    DLOG(INFO) << "=====================================================================";
  t0.Stop();
  DLOG(INFO) << "!!Total Duration for Put Key " << key << " blk idx " << blk_idx 
             << " " << t0.ElapsedMicroseconds() << " us "
             << " Search for Updated vid: " << t1.ElapsedMicroseconds()  << " us "
             << " Marshalling: " << t2.ElapsedMicroseconds() << " us "
             << " Put to Storage: " << t3.ElapsedMicroseconds() << " us "
             << " Dependency Checking: " << depTimer.ElapsedMicroseconds() << " us "
             << " Build DASL " << daslTimer.ElapsedMicroseconds() << " us "
             << " Debug Timer: " << debugTimer.ElapsedMicroseconds() << " us ";
#endif
  return true;
}


// Implementation of Algorithm 2 in Paper
bool KVDB::DASLAppend(const std::string& key, ull blk_idx, 
                      std::vector<ull>* pre_blks, 
                      std::vector<std::string>* pre_vids) {
  auto head_result = odb_.GetBranchHead(ustore::Slice(key), DEFAULT_BRANCH);
  if (NotExists(head_result.stat)) {
    DLOG(INFO) << "Key " << key << " does not previously exist.";
    return true;
  } else if (head_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to get Branch head for key " << key << " with error code " << head_result.stat;
  }

  Hash cur_vid = head_result.value.Clone();
  bool finish = false;
  size_t level = 0;
  
  while (!finish) {
    DLOG(INFO) << "Fetch for Key " << key << " vid " << cur_vid.ToBase32();
    auto result = odb_.Get(ustore::Slice(key), cur_vid);
    if (result.stat != ustore::ErrorCode::kOK) {
      LOG(FATAL) << "Fail to fetch entry for " << key << " with vid " 
                  << cur_vid.ToBase32();
    }

    std::string cur_encoded = result.value.cell().context().ToString();
    ull cur_blk_idx;
    std::vector<ull> cur_pre_blks;
    std::vector<std::string> cur_pre_vids;

    if (!UnmarshalProv(cur_encoded, &cur_blk_idx, nullptr,
                      nullptr, nullptr, nullptr, nullptr, 
                      &cur_pre_blks, &cur_pre_vids)) {
      LOG(FATAL) << "Fail to unmarshall context for entry with Key " << key
                 << " vid " << cur_vid.ToBase32();
    } else {
      DLOG(INFO) << "Successfully unmarshall context [" << cur_encoded << "] for entry with Key " << key
                 << " vid " << cur_vid.ToBase32();
      for (size_t i = 0;i < cur_pre_blks.size(); i++) {
        DLOG(INFO) << "     " << cur_pre_vids[i]  << ": " << cur_pre_blks[i];
      }
    }

    size_t l = cur_pre_blks.size();
    if (l > 0) {
      for (size_t j = level; j < l; j++) {
        if (cur_blk_idx / power(this->base_, j) < blk_idx / power(this->base_, j)) {
          pre_blks->push_back(cur_blk_idx);
          pre_vids->push_back(cur_vid.ToBase32());
        } else {
          finish = true;
          break;
        }  // end if
      }  // end for
      if (!finish) {
        DLOG(INFO) << "Prepare to create hash with vid " << cur_pre_blks.back();
        cur_vid = Hash::FromBase32(cur_pre_vids.back()).Clone();
        DLOG(INFO) << "Finish create hash with vid " << cur_vid.ToBase32();
        CHECK_EQ(base_, 2);
        level = l;  
      }   // end if finish
    } else {
      DLOG(INFO) << "Reaches the first entry for Key " << key << " at level " << level;
      // this->base_ = 2;
      while (cur_blk_idx / power(this->base_, level) < blk_idx / power(this->base_, level)) {
        DLOG(INFO) << "  level: " << level << " base: " << this->base_ << " cur_blk_idx: " <<  cur_blk_idx << " blk_idx: " << blk_idx;
        DLOG(INFO) << "  a1: " << cur_blk_idx / power(this->base_, level) << " a2: " << blk_idx / power(this->base_, level);
        pre_blks->push_back(cur_blk_idx);
        pre_vids->push_back(cur_vid.ToBase32());
        ++level;
      } //
      DLOG(INFO) << "Finish while when working with the first entry...";
      finish = true;
    }  // end if l > 0
  }  // end while
#ifdef DEBUG
  DLOG(INFO) << "Key " << key << " with blk idx " << blk_idx << " has level " << level;
	CHECK(pre_blks->size() == pre_vids->size());
  for (size_t d = 0; d < pre_blks->size(); ++d) { 
    DLOG(INFO) << "  Pre Blk " << (*pre_blks)[d] << " vid " << (*pre_vids)[d];
  }
#endif
  return true;
}

std::pair<Status, std::string> KVDB::Commit() {

  DLOG(INFO) << "Start Commit...";
  DLOG(INFO) << "Prepare for the update of forward info...";
  // Correctly update the new forward dependency information for the current block to the storage
  for (const auto& temp_forward_info : this->temp_forward_key_vids_) {
    std::string prov_key = temp_forward_info.first + "-prov";
    std::string prev_forward_info;
    if (odb_.Exists(Slice(prov_key)).value) {
      auto forward_result = odb_.Get(Slice(prov_key), DEFAULT_BRANCH);
      if (forward_result.stat == ErrorCode::kOK) {
        CHECK(forward_result.value.type() == UType::kString);
        auto str = forward_result.value.String();
        prev_forward_info.clear();
        prev_forward_info.reserve(str.len());
        prev_forward_info.append(reinterpret_cast<const char*>(str.data()), str.len());
      } else {
        LOG(FATAL) << "Fail to retrieve prov key " << prov_key << " with error code " << forward_result.stat;
      }
    } else {
      prev_forward_info = "";
    }
    std::string cur_forward_info = prev_forward_info + temp_forward_info.second;
    DLOG(INFO) << "Forward info for " << prov_key << " shifts from [" 
               <<  prev_forward_info + "] to [" << cur_forward_info << "]";

    VString val{Slice{cur_forward_info}};
    auto put_ret = odb_.Put(Slice(prov_key), val, DEFAULT_BRANCH);
    if (put_ret.stat == ErrorCode::kOK) {
      DLOG(INFO) << "  Successfuly update prov key " << prov_key;
    } else {
      LOG(FATAL) << "Fail to put prov key " << prov_key << " with error code " << put_ret.stat;
    }
  }  // end for
  this->temp_forward_key_vids_.clear();

  DLOG(INFO) << "Prepare to Commit the global map...";
  std::vector<Slice> slice_key, slice_val;
  std::string updates = "\n";
  std::string empty = "";
  for (auto it = updated_vids_.begin(); 
      it != updated_vids_.end(); ++it) {
    slice_key.push_back(Slice(it->first));
    slice_val.push_back(Slice(it->second));
    updates += it->first + ": " + it->second + "\n";

    // Empty the forward dependency info for each updated key
    VString val{Slice{empty}};
    std::string prov_key = it->first + "-prov";
    auto put_ret = odb_.Put(Slice(prov_key), val, DEFAULT_BRANCH);
    if (put_ret.stat == ErrorCode::kOK) {
      DLOG(INFO) << "  Successfuly empty for prov key " << prov_key;
    } else {
      LOG(FATAL) << "Fail to empty prov key with error code " << put_ret.stat;
    }
    
  }
  global_.Set(slice_key, slice_val); 
  DLOG(INFO) << "Prepare to Put Global Map with " << slice_key.size() << " updates: " << updates;

  auto result = odb_.Put(GLOBAL_STATE_KEY, global_, DEFAULT_BRANCH);
  if (result.stat != ustore::ErrorCode::kOK) { 
    DLOG(INFO) << "Fail to put global state with error code " << result.stat;
    LOG(FATAL) << "Fail to put global state with error code " << result.stat;
  } else {
    DLOG(INFO) << "successfully put global state with error code " << result.stat;
  }

  DLOG(INFO) << "Prepare to Get Global Map";
  auto fetch_result = odb_.Get(GLOBAL_STATE_KEY, DEFAULT_BRANCH);
  if (fetch_result.stat != ErrorCode::kOK) {
    DLOG(INFO) << "Fail to fetch for the latest state map";
    LOG(FATAL) << "Fail to fetch for the latest state map";
  } else {
    DLOG(INFO) << "Successfully get latest global map";
  }
  global_ = std::move(fetch_result.value.Map());

  this->updated_vids_.clear();
  std::string map_vid = result.value.ToBase32();
  DLOG(INFO) << "Finish Committing Global Map with vid " << map_vid;
  return {Status::OK(), map_vid};
}


std::string KVDB::MarshalProv(ull blk_idx, const std::string& txnID,
                              const std::vector<std::string>& for_keys,
                              const std::vector<std::string>& for_vids,
                              const std::vector<std::string>& dep_keys,
                              const std::vector<std::string>& dep_vids,
                              const std::vector<ull>& pre_blks,
                              const std::vector<std::string>& pre_vids, 
                              int self_dependent) {

  std::ostringstream oss;
  CHECK_EQ(for_keys.size(), for_vids.size());
  CHECK_EQ(dep_keys.size(), dep_vids.size());
  // DLOG(INFO) << "=============Encoding=============";
  // DLOG(INFO) << "Block_Idx: " << blk_idx << std::endl;
  oss << blk_idx << " ";
  oss << txnID << " ";
  // DLOG(INFO) << "TxnID: " << txnID << std::endl;
  size_t num_forward = for_keys.size();
  oss << num_forward << " ";

  for (size_t i = 0; i < num_forward; ++i) {
    oss << for_keys[i] << " ";
    oss << for_vids[i] << " ";
  }

  size_t num_dep = dep_keys.size();
  // DLOG(INFO) << "# Of Dependent Keys: "
  //            << num_dep << std::endl;
  oss << num_dep << " ";

  // Here, we assume no space allowed in any key string
  // DLOG(INFO) << "Dependency: ";
  for (size_t i = 0; i < num_dep; ++i) {
    // DLOG(INFO) << dep_keys[i] << ": " << dep_vids[i] << std::endl;
    oss << dep_keys[i] << " ";
    oss << dep_vids[i] << " ";
  }

  size_t num_pre = pre_vids.size();
  oss << num_pre << " ";

  for (size_t i = 0; i < num_pre; ++i) {
    oss << pre_blks[i] << " ";
    oss << pre_vids[i] << " ";
  }
  oss << self_dependent;
  // DLOG(INFO) << "==================================";
  return oss.str();
}


bool KVDB::UnmarshalProv(std::string encoded,
                         ull* blk_idx,
                         std::string* txnID,
                         std::vector<std::string>* for_keys,
                         std::vector<std::string>* for_vids,
                         std::vector<std::string>* dep_keys,
                         std::vector<std::string>* dep_vids,
                         std::vector<ull>* pre_blks,
                         std::vector<std::string>* pre_vids, 
                         int* self_dependent) {
  std::istringstream iss(encoded);
  ull blkidx;
  iss >> blkidx;
  if (blk_idx) {*blk_idx = blkidx;}

  std::string txn_id;
  iss >> txn_id;
  if (txnID) { *txnID = txn_id; }

  size_t num_forward;
  iss >> num_forward;

  for (size_t i = 0; i < num_forward; ++i) {
    std::string for_key;
    iss >> for_key;
    std::string for_vid;
    iss >> for_vid;
    if (for_keys) for_keys->emplace_back(for_key);
    if (for_vids) for_vids->emplace_back(for_vid);
  }

  size_t num_dep;
  iss >> num_dep;

  for (size_t i = 0; i < num_dep; ++i) {
    std::string dep_key;
    iss >> dep_key;
    std::string dep_vid;
    iss >> dep_vid;
    if (dep_keys) dep_keys->emplace_back(dep_key);
    if (dep_vids) dep_vids->emplace_back(dep_vid);
  }  // end for

  size_t num_pre;
  iss >> num_pre;

  for (size_t i = 0; i < num_pre;++i) {
    ull pre_blk;
    iss >> pre_blk;
    std::string pre_vid;
    iss >> pre_vid;
    if (pre_blks)  pre_blks->emplace_back(pre_blk);
    if (pre_vids) pre_vids->emplace_back(pre_vid);
  }

  if (self_dependent) { iss >> *self_dependent;}
  return true;
}
std::pair<std::string, std::string> KVDB::LowerUpperBoundVid(const std::string& key, ull queried_blk_idx
  , ErrorCode* code) {
  auto head_result = odb_.GetBranchHead(ustore::Slice(key), DEFAULT_BRANCH);
  if (NotExists(head_result.stat)) {
    LOG(WARNING) << "Key " << key << " does not exist.";
    *code = head_result.stat;
    return {"", ""};
  } else if (head_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Get Branch Head fails for key " << key << " with error code " << head_result.stat;
  }

  Hash cur_hash = head_result.value;
  auto fetch_result = odb_.Get(ustore::Slice(key), cur_hash);
  if (fetch_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to fetch entry for Key " << key << " with vid " << cur_hash.ToBase32()
               << " with error code " << fetch_result.stat;
  }
  auto context = fetch_result.value.cell().context().ToString();
  ull blk_idx;
  std::vector<ull> pre_blk_idxs;
  std::vector<std::string> pre_vids;
  if (!UnmarshalProv(context, &blk_idx, nullptr, nullptr, nullptr, 
                      nullptr, nullptr,&pre_blk_idxs, &pre_vids)) {
    LOG(FATAL) << "Fail to unmarshall context for Key " 
               << key << " with vid " << cur_hash.ToBase32();
  }
  DLOG(INFO) << "Fetch head for key " << key << " with vid " << cur_hash.ToBase32() << " with blk_idx " << blk_idx;

  if (blk_idx <= queried_blk_idx) {
    *code = ErrorCode::kOK;
    DLOG(INFO) << "cur blk idx: " << blk_idx 
    << ". no entry with blk idx strictly exceeds queried_blk_idx" << queried_blk_idx;
    return {cur_hash.ToBase32(), ""};
  }

#ifdef GO_USE_DASL
// Query with DASL, which has no difference to a normal skip list but starts from the last node
  while (true) {
    // precondition: the blk_idx of cur_hash strictly greater than queried_blk_idx
    std::string next_vid = "";
    // LOG(INFO) << "In the while loop LowerUpperBoundVid()";
    for (size_t i = 0;i < pre_blk_idxs.size(); i++) {
      if (pre_blk_idxs[i] <= queried_blk_idx) {
        break;
      } else {
        next_vid = pre_vids[i];
      } // end if
    }  // end for
    if (pre_blk_idxs.size() == 0) {
      DLOG(INFO) << "Reaches the first entry";
      *code = ErrorCode::kOK;
      return {"", cur_hash.ToBase32()};
    } else if (next_vid == "") {
      DLOG(INFO) <<  "all preceding entries have blk_idx smaller or equal to queried_blk_idx";
      //   cur_hash is the upper bound. And the previous is lower bound. 
      *code = ErrorCode::kOK;
      return {pre_vids[0], cur_hash.ToBase32()};
    } else {
      cur_hash = Hash::FromBase32(next_vid).Clone();
      auto fetch_result = odb_.Get(ustore::Slice(key), cur_hash);
      if (fetch_result.stat != ustore::ErrorCode::kOK) {
        LOG(FATAL) << "Fail to fetch entry for Key " << key << " with vid " << cur_hash.ToBase32()
                  << " with error code " << fetch_result.stat;
      }
      pre_blk_idxs.clear();
      pre_vids.clear();
      auto context = fetch_result.value.cell().context().ToString();

      if (!UnmarshalProv(context, &blk_idx, nullptr, nullptr, nullptr, 
                          nullptr, nullptr,&pre_blk_idxs, &pre_vids)) {
        LOG(FATAL) << "Fail to unmarshall context for Key " 
                  << key << " with vid " << cur_hash.ToBase32();
      }
      DLOG(INFO) << " Fetch entry for Key " << key << " vid " << cur_hash.ToBase32() << " with blk idx " << blk_idx;
    }
  }
#else
// Without using DASL, do the linear scan
  while(true) {
    // precondition: the blk_idx of fetch_result <cur_hash> strictly greater than queried_blk_idx
    ull blk_idx = 0;
    auto pre_hash = fetch_result.value.cell().preHash();
    if (pre_hash == Hash::kNull) {
      *code = ErrorCode::kOK;
      return {"", cur_hash.ToBase32()};
    } 

    auto pre_fetch_result = odb_.Get(ustore::Slice(key), pre_hash);
    if (pre_fetch_result.stat != ustore::ErrorCode::kOK) {
      LOG(FATAL) << "Fail to fetch entry for Key " << key << " with vid " << pre_hash.ToBase32()
                << " with error code " << pre_fetch_result.stat;
    }
    auto context = pre_fetch_result.value.cell().context().ToString();
    if (!UnmarshalProv(context, &blk_idx)) {
      LOG(FATAL) << "Fail to unmarshall context for Key " 
                << key << " with vid " << pre_hash.ToBase32();
    }
    DLOG(INFO) << "Context: " << context;

    DLOG(INFO) << " Fetch entry for Key " << key << " vid " << pre_hash.ToBase32() << " with blk idx " << blk_idx;
    if (blk_idx <= queried_blk_idx) {
      *code = ErrorCode::kOK;
      return {pre_hash.ToBase32(), cur_hash.ToBase32()};
    }
    fetch_result = std::move(pre_fetch_result);
    cur_hash = pre_hash.Clone();
  }
#endif
  LOG(FATAL) << "Should not reach here";
  return {"", ""};
}


HistReturn KVDB::Hist(const std::string& key, ull blk_idx) {
  ErrorCode return_code;
  DLOG(INFO) << "Start Hist Query on key " << key << " at blk idx " << blk_idx;
  auto lower_upper = LowerUpperBoundVid(key, blk_idx, &return_code);                                    
  DLOG(INFO) << "Finish LowerUpperBoundVid for key " << key;
  if (NotExists(return_code)) {
    return {Status::NotFound("Not found key " + key), 0, ""};
  }

  if (lower_upper.first == "") {
    return {Status::NotFound("No entry for key " + key + " with blk idx smaller or equal to " + std::to_string(blk_idx)), 0, ""};
  }

  auto fetch_result = odb_.Get(ustore::Slice(key), Hash::FromBase32(lower_upper.first));
  if (fetch_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to fetch entry for Key " << key << " with vid " << lower_upper.first
              << " with error code " << fetch_result.stat;
  } else {
    DLOG(INFO) << "Successfully fetch result for key " << key;
  }

  std::string context = fetch_result.value.cell().context().ToString();
  DLOG(INFO) << "Context: " << context;
  ull dump_blk_idx;
  if (!UnmarshalProv(context, &dump_blk_idx)) {
    LOG(FATAL) << "Fail to unmarshall context for Key " 
              << key << " with vid " << lower_upper.first;
  }

  DLOG(INFO) << " Fetch entry for Key " << key << " vid " << lower_upper.first << " with blk idx " << dump_blk_idx;

  std::string value;
  auto str = fetch_result.value.String();
  value.clear();
  value.reserve(str.len());
  value.append(reinterpret_cast<const char*>(str.data()), str.len());
  DLOG(INFO) << "Finish Hist Query on key " << key << " at blk idx " << blk_idx;
  return {Status::OK(), dump_blk_idx, value};
}

BackwardReturn KVDB::Backward(const std::string& key, ull blk_idx) {
  ErrorCode return_code;
  auto lower_upper = LowerUpperBoundVid(key, blk_idx, &return_code);                                      

  if (NotExists(return_code)) {
    auto status = Status::NotFound("Not found key " + key);
    return BackwardReturn(status);
  }

  if (lower_upper.first == "") {
    auto status = Status::NotFound("No entry for key " + key + " with blk idx smaller or equal to " + std::to_string(blk_idx));
    return BackwardReturn(status);
  }

  auto fetch_result = odb_.Get(ustore::Slice(key), Hash::FromBase32(lower_upper.first));
  if (fetch_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to fetch entry for Key " << key << " with vid " << lower_upper.first
              << " with error code " << fetch_result.stat;
  }

  std::string context = fetch_result.value.cell().context().ToString();

  ull dump_blk_idx;
  std::vector<std::string> dep_keys;
  std::vector<std::string> dep_vids;
  std::vector<ull> dep_blk_idxs;
  std::string txnID;
  if (!UnmarshalProv(context, &dump_blk_idx, &txnID, nullptr, nullptr, &dep_keys, &dep_vids)) {
    LOG(FATAL) << "Fail to unmarshall context for Key " 
              << key << " with vid " << lower_upper.first;
  }

  BackwardReturn br(Status::OK(), txnID);
  for (size_t i = 0;i < dep_keys.size(); i++) {
    auto dep_fetch_result = odb_.Get(ustore::Slice(dep_keys[i]), Hash::FromBase32(dep_vids[i]));
    if (dep_fetch_result.stat != ustore::ErrorCode::kOK) {
      LOG(FATAL) << "Fail to fetch entry for Dep Key " << dep_keys[i] << " with vid " << dep_vids[i] 
                << " with error code " << dep_fetch_result.stat;
    }

    std::string dep_context = dep_fetch_result.value.cell().context().ToString();
    ull dep_blk_idx;
    if (!UnmarshalProv(dep_context, &dep_blk_idx)) {
      LOG(FATAL) << "Fail to unmarshall context for Key " 
                << dep_keys[i] << " with vid " << dep_vids[i];
    }

    br.AddKeyBlk(dep_keys[i], dep_blk_idx);
  }
  return br;
}

ForwardReturn KVDB::Forward(const std::string& key, ull blk_idx) {
  ErrorCode return_code;
  auto lower_upper = LowerUpperBoundVid(key, blk_idx, &return_code);                                      

  if (NotExists(return_code)) {
    auto status = Status::NotFound("Not found key " + key);
    return ForwardReturn(status);
  }

  if (lower_upper.first == "") {
    auto status = Status::NotFound("No entry for key " + key + " with blk idx smaller or equal to " + std::to_string(blk_idx));
    return ForwardReturn(status);
  }
  

  std::vector<std::string> for_keys;
  std::vector<std::string> for_vids;
  if (lower_upper.second == "") { 
    // the queried key vid is up-to-date, the forward info is buffered in the storage. 
    std::string prov_key = key + "-prov";
    auto forward_result = odb_.Get(Slice(prov_key), DEFAULT_BRANCH);
    if (forward_result.stat == ErrorCode::kKeyNotExists) {
      // do nothing
    } else if (forward_result.stat == ErrorCode::kOK) {
      CHECK(forward_result.value.type() == UType::kString);
      auto str = forward_result.value.String();
      std::string prev_forward_info;
      prev_forward_info.clear();
      prev_forward_info.reserve(str.len());
      prev_forward_info.append(reinterpret_cast<const char*>(str.data()), str.len());

      auto splits = split(prev_forward_info, " ");
      CHECK_EQ("", splits[0]);
      CHECK_EQ(1, splits.size() % 2);

      for (size_t i = 1; i <= splits.size() / 2; i++) {
        std::string forward_key = splits[2*i-1];
        std::string forward_vid = splits[2*i];
        DLOG(INFO) << "  Forward key " << forward_key << " vid " << forward_vid;
        for_keys.push_back(forward_key);
        for_vids.push_back(forward_vid);
      }
    } else {
      LOG(FATAL) << "Fail to retrieve prov key " << prov_key << " with error code " << forward_result.stat;
    }
  } else {
    auto fetch_result = odb_.Get(ustore::Slice(key), Hash::FromBase32(lower_upper.second));
    if (fetch_result.stat != ustore::ErrorCode::kOK) {
      LOG(FATAL) << "Fail to fetch entry for Key " << key << " with vid " << lower_upper.second
                << " with error code " << fetch_result.stat;
    }

    int self_dep;
    std::string context = fetch_result.value.cell().context().ToString();
    if (!UnmarshalProv(context, nullptr, nullptr, &for_keys, &for_vids, nullptr, nullptr, nullptr, nullptr, &self_dep)) {
      LOG(FATAL) << "Fail to unmarshall context for Key " 
                << key << " with vid " << lower_upper.second;
    }
    if (self_dep > 0) {
      for_keys.push_back(key);
      for_vids.push_back(lower_upper.second);
    }
  }

  ForwardReturn fr(Status::OK());
  for (size_t i = 0; i < for_keys.size(); i++) {
    ull for_blk_idx;
    std::string for_txnID;

    auto for_fetch_result = odb_.Get(ustore::Slice(for_keys[i]), Hash::FromBase32(for_vids[i]));
    if (for_fetch_result.stat != ustore::ErrorCode::kOK) {
      LOG(FATAL) << "Fail to fetch entry for Dep Key " << for_keys[i] << " with vid " << for_vids[i] 
                << " with error code " << for_fetch_result.stat;
    }

    std::string for_context = for_fetch_result.value.cell().context().ToString();
    if (!UnmarshalProv(for_context, &for_blk_idx, &for_txnID)) {
      LOG(FATAL) << "Fail to unmarshall context for Key " 
                << for_keys[i] << " with vid " << for_vids[i];
    }
    fr.AddTxnIDKeyBlk(for_txnID, for_keys[i], for_blk_idx);
  }

  return fr;
}

ull KVDB::GetLatestVersion(const std::string& key) {
  auto fetch_result = odb_.Get(ustore::Slice(key), DEFAULT_BRANCH);
  if (NotExists(fetch_result.stat)) {
    LOG(WARNING) << "Key" << key << " not exists...";
  } else if (fetch_result.stat != ustore::ErrorCode::kOK) {
    LOG(FATAL) << "Fail to fetch latest entry for Key " << key
               << " with error code " << fetch_result.stat;
  }

  std::string context = fetch_result.value.cell().context().ToString();
  ull blk_idx;
  if (!UnmarshalProv(context, &blk_idx)) {
    LOG(FATAL) << "Fail to unmarshall context for Key " << key;
  }
  return blk_idx; 
}

}  // namespace ustore_kvdb
