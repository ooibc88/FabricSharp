%module ustore
%include <std_map.i>
%include <std_string.i>
%include <std_pair.i>
%include <std_vector.i>

%{
#include "db.h"
%}

%template(PairStatusString) std::pair<ustore_kvdb::Status, std::string>;
%template(VecStr) std::vector<std::string>;
%template(VecUll) std::vector<unsigned long long>;

namespace ustore_kvdb {

%nodefaultctor HistReturn;
class HistReturn;

%nodefaultctor BackwardReturn;
class BackwardReturn;

%nodefaultctor ForwardReturn;
class ForwardReturn;

class KVDB {
 public:
  explicit KVDB(unsigned int id = 42);
  void OutputChunkStorage();

  std::pair<Status, std::string> Get(const std::string& key);
  Status Put(const std::string& key, const std::string& value);

  std::pair<Status, std::string> GetBlock(const std::string& key,
                                          const std::string& version);

  std::pair<Status, std::string> PutBlock(const std::string& key,
                                          const std::string& value);

  void IterateState(const std::string& key);
  
  Status InitGlobalState();

  std::pair<Status, std::string> Commit();

  bool PutState(const std::string& key, const std::string& val, const std::string& txnID,
                unsigned long long blk, const std::vector<std::string>& deps);

  HistReturn Hist(const std::string& key, unsigned long long blk_idx); 

  BackwardReturn Backward(const std::string& key, unsigned long long blk_idx); 

  ForwardReturn Forward(const std::string& key, unsigned long long blk_idx); 
  
  unsigned long long GetLatestVersion(const std::string& key);
};

class HistReturn {
 public:
  Status status();
  unsigned long long blk_idx();
  std::string value();
};

class BackwardReturn {
 public:
  Status status();
  std::string txnID();
  std::vector<std::string> dep_keys();
  std::vector<unsigned long long> dep_blk_idx();
};

class ForwardReturn {
 public:
   Status status();
   std::vector<std::string> txnIDs();
   std::vector<std::string> forward_keys();
   std::vector<unsigned long long> forward_blk_idx();
};
}
