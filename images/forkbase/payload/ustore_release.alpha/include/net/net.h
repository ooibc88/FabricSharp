// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NET_NET_H_
#define USTORE_NET_NET_H_

#include <string>
#include <unordered_map>
#include <vector>
#include "types/type.h"
#include "utils/noncopyable.h"
#include "utils/logging.h"

namespace ustore {

using node_id_t = std::string;
const char kCloseMsg[] = "+close";

/**
 * Callback functor that is invoked on receiving of a message.
 * The network thread will free the message after calling this, so if
 * the message is to be processed asynchronously, a copy must be made.
 * handler: is the pointer to the object that processes the message
 *          it is "registered" via the Net object (RegisterRecv)
 * source:  extracted from the received socket
 */
class CallBack {
 public:
  explicit CallBack(void* handler): handler_(handler) {}
  ~CallBack() {}
  virtual void operator()(const void* msg, int size,
                          const node_id_t& source) = 0;

 protected:
  void* handler_;
};

class NetContext;

/**
 * Wrapper to all network connections. This should be created only once for each network
 * implementation (either TCP or RDMA).
 */
class Net : private Noncopyable {
 public:
  virtual ~Net();

  // create the NetContext of idth node
  virtual NetContext* CreateNetContext(const node_id_t& id) = 0;

  // delete the NetContext
  virtual void DeleteNetContext(NetContext* ctx);
  inline void DeleteNetContext(const node_id_t& id) {
    // CHECK(ContainNetContext(id));
    if (netmap_.count(id)) {
      DeleteNetContext(netmap_.at(id));
      netmap_.erase(id);
      DLOG(INFO) << "Delete NetContext " << id;
    }
  }

  // create the NetContexts of all the nodes
  void CreateNetContexts(const std::vector<node_id_t>& nodes);
  virtual void Start() = 0;  // start the listening service
  virtual void Stop() = 0;  // stop the listening service
  inline bool IsRunning() const noexcept { return is_running_; }

  // get the NetContext of the idth node
  inline const node_id_t& GetNodeID() const { return cur_node_; }
  inline NetContext* GetNetContext(const node_id_t& id) const {
    return netmap_.at(id);
  }

  inline bool ContainNetContext(const node_id_t& id) const {
    return netmap_.count(id) == 1 ? true : false;
  }

  /**
   * Register the callback function that will be called whenever there is new
   * data is received
   * func: the callback function
   * handler: pointer to a object that handle this request
   *
   * anh: originally this is a member of NetContext. But moved here to make
   * it cleaner. First, it needs to registered only once, instead of for as many as
   * the number of connections. Second, there is no concurrency problem, because the
   * received socket is read by only one (the main) thread.
   */
  inline void RegisterRecv(CallBack* cb) {
    cb_ = cb;
  }

 protected:
  Net() {}
  explicit Net(const node_id_t& id) : cur_node_(id) {}
  node_id_t cur_node_;
  std::unordered_map<node_id_t, NetContext*> netmap_;
  CallBack* cb_ = nullptr;
  volatile bool is_running_;
};

/**
 * Generic network context representing one end-to-end connection.
 */
class NetContext {
 public:
  NetContext() = default;
  // Initialize connection to another node
  NetContext(const node_id_t& src, const node_id_t& dest)
      : src_id_(src), dest_id_(dest) {}
  virtual ~NetContext() = default;

  // Non-blocking APIs
  /*
   * send the data stored in ptr[0, len-1]
   * ptr: the starting pointer to send
   * len: size of the buffer
   * func: callback function after send completion (not supported)
   * handler: the application-provided handler that will be used in the callback
   *           function (not supported)
   */
  virtual ssize_t Send(const void* ptr, size_t len,
                       CallBack* func = nullptr) = 0;


  // Blocking APIs (not supported)
  virtual ssize_t SyncSend(const void* ptr, size_t len);
  virtual ssize_t SyncRecv(const void* ptr, size_t len);

  // methods to access private variables
  inline const node_id_t& srcID() const noexcept { return src_id_; }
  inline const node_id_t& destID() const noexcept { return dest_id_; }

 protected:
  node_id_t src_id_, dest_id_;
};

namespace net {

  // create network instance, caller is responsible for the allocated instance
  Net* CreateServerNetwork(const node_id_t& id, int n_threads = 1);
  Net* CreateClientNetwork(int n_threads = 1);

}  // namespace net
}  // namespace ustore

#endif  // USTORE_NET_NET_H_
