// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NET_RDMA_NET_H_
#define USTORE_NET_RDMA_NET_H_

#ifdef USE_RDMA

#include <infiniband/verbs.h>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <syscall.h>
#include <atomic>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sstream>
#include "net/hashtable.h"
#include "net/net.h"
#include "net/ae.h"
#include "utils/logging.h"
#include "net/rdma_config.h"
#include "utils/singleton.h"

namespace ustore {

aeFileProc TCPConnectionHandle;
aeFileProc TcpHandle;
aeFileProc RdmaHandle;
class RdmaNetContext;
class RdmaNet;

class RdmaNetResource {
  friend class RdmaNetContext;
 public:
  explicit RdmaNetResource(ibv_device *device);
  ~RdmaNetResource();

  inline const char* GetDevname() const { return this->devName_; }
  inline ibv_cq* GetCompQueue() const noexcept { return cq_; }
  inline int GetChannelFd() const { return channel_->fd; }

  bool GetCompEvent() const;
  int RegLocalMemory(void *base, size_t sz);
  int RegCommSlot(int);
  char* GetSlot(int s) const;  // get the starting addr of the slot
  int PostRecv(int n);  // post n RR to the srq
  int PostRecvSlot(int slot);  // post slot to the srq
  // int ClearRecv(int low, int high);

  RdmaNetContext* NewRdmaNetContext(node_id_t id);
  void DeleteRdmaNetContext(RdmaNetContext* ctx);

  inline void ClearSlot(int s) { slots_.at(s) = false; }
  inline int GetCounter() const noexcept { return rdma_context_counter_; }

 private:
  ibv_device *device_;
  const char *devName_ = NULL;
  ibv_context *context_;
  ibv_comp_channel *channel_;
  ibv_pd *pd_;
  ibv_cq *cq_;  // global comp queue
  ibv_srq *srq_;  // share receive queue

  ibv_port_attr portAttribute_;
  int ibport_ = 1;  // TODO(zhanghao): dual-port support
  uint32_t psn_;

  // the follow three variables are only used for comm among workers
  void* base_ = nullptr;  // the base addr for the local memory
  size_t size_ = 0;
  struct ibv_mr *bmr_ = nullptr;

  // node-wide communication buf used for receive request
  std::vector<struct ibv_mr*> comm_buf_;
  // size_t buf_size; buf_size = slots.size() * MAX_REQUEST_SIZE
  int slot_head_;  // current slot head
  int slot_inuse_;  // number of slots in use
  // TODO(zhanghao): check whether head + tail is enough
  // the states of all the allocated slots (true: occupied, false: free)
  std::vector<bool> slots_;
  // current created RdmaNetContext
  int rdma_context_counter_;

  std::atomic<int> recv_posted_;
  int rx_depth_ = 0;
};

// RDMA implementation of network context
class RdmaNetContext : public NetContext {
  friend class RdmaNet;
 public:
  RdmaNetContext(const node_id_t& id, RdmaNetResource* res);
  ~RdmaNetContext();

  // implementation of the methods inherited from NetContext
  ssize_t Send(const void* ptr, size_t len, CallBack* func = nullptr) override;

  /**
   * Memory API
   * @dest: dest addr at remote node
   * @src: src addr at local node
   */
  ssize_t Write(raddr dest, raddr src, size_t len, CallBack* func = nullptr);
  ssize_t WriteWithImm(raddr dest, raddr src, size_t len, uint32_t imm,
                       CallBack* func = nullptr);
  ssize_t Read(raddr dest, raddr src, size_t len, CallBack* func = nullptr);
  ssize_t WriteBlocking(raddr dest, raddr src, size_t len);
  ssize_t WriteWithImmBlocking(raddr dest, raddr src, size_t len, uint32_t imm);
  ssize_t ReadBlocking(raddr dest, raddr src, size_t len);

  // for internel use
  int SetRemoteConnParam(const char *remConn);
  const char* GetRdmaConnString();

  inline void SetFD(int fd) {
    CHECK(fd);
    if (sockfds_[0]) {
      CHECK(!sockfds_[1]);
      sockfds_[1] = fd;
    } else {
      sockfds_[0] = fd;
    }
  }

 private:
  // below are for internal use
  int ExchConnParam(const node_id_t& cur_node,
                    const node_id_t& node, RdmaNet* net);
  inline uint32_t GetQP() const { return qp_->qp_num; }
  inline const node_id_t& GetID() const { return id_; }

  unsigned int SendComp(ibv_wc& wc);
  unsigned int WriteComp(ibv_wc& wc);
  char* RecvComp(ibv_wc& wc);
  char* GetFreeSlot();

  inline int PostRecv(int n) { return resource_->PostRecv(n);}
  inline void lock() { global_lock_.lock(); }
  inline void unlock() { global_lock_.unlock(); }

  char* GetFreeSlot_();
  void ProcessPendingRequests(int n);
  bool IsRegistered(const void* addr);

  ssize_t Rdma(ibv_wr_opcode op, const void* src, size_t len,
               unsigned int id = 0, bool signaled = false, void* dest = nullptr,
               uint32_t imm = 0, uint64_t oldval = 0, uint64_t newval = 0);
  ssize_t Rdma(RdmaRequest& r);
  ssize_t SendGeneric(const void* ptr, size_t len, unsigned int id = 0,
                bool signaled = false);
  /*
   * @dest: dest addr at remote node
   * @src: src addr at local node
   */
  ssize_t WriteGeneric(raddr dest, raddr src, size_t len, unsigned int id = 0,
                 bool signaled = false);
  ssize_t WriteWithImmGeneric(raddr dest, raddr src, size_t len, uint32_t imm,
                        unsigned int id = 0, bool signaled = false);
  /*
   * @dest: dest addr at local node
   * @src: src addr at remote node
   */
  ssize_t ReadGeneric(raddr dest, raddr src, size_t len, unsigned int id = 0,
                bool signaled = false);
  ssize_t CasGeneric(raddr src, uint64_t oldval, uint64_t newval,
                     unsigned int id = 0, bool signaled = false);

  RdmaNetResource *resource_;
  ibv_qp *qp_;
  node_id_t id_;
  ibv_mr* send_buf_;  // send buf
  int slot_head_;
  int slot_tail_;
  // to differentiate between all free and all occupied slot_head == slot_tail
  bool full_;

  uint64_t vaddr_ = 0;  // for remote rdma read/write
  uint32_t rkey_ = 0;

  int max_pending_msg_;
  int max_unsignaled_msg_;
  // including both RDMA send and write/read that don't use the send buf
  std::atomic<int> pending_msg_;
  // including only send msg
  std::atomic<int> pending_send_msg_;
  // in order to proceed the slot_tail
  std::atomic<int> to_signaled_send_msg_;
  std::atomic<int> to_signaled_w_r_msg_;
  std::queue<RdmaRequest> pending_requests_;
  char *msg_ = nullptr;
  std::mutex global_lock_;

  /*
   * the TCP fd used to transfer rdma connection info
   * in case of concurrent connecting from workers,
   * we use two fds to deal with this issue
   */
  int sockfds_[2] = {0, 0};
};


class RdmaNet : public Net {
 public:
  explicit RdmaNet(const node_id_t& id, int nthreads = 1);
  ~RdmaNet();

  NetContext* CreateNetContext(const node_id_t& id) override;

  inline void DeleteNetContext(NetContext* ctx) override {
    RdmaNetContext* rctx = dynamic_cast<RdmaNetContext*>(ctx);
    LOG(WARNING) << "Delete RdmaNetContext " << rctx->GetID();
    if (qpCliMap_.erase(rctx->GetQP()))
      resource_->DeleteRdmaNetContext(rctx);
  }

  using Net::DeleteNetContext;

  void Start() override {
    is_running_ = true;
    while (is_running_) {}
  }
  void Stop() override {
    is_running_ = false;
    aeStop(el_);
  }

  // below are for internal use
  RdmaNetContext* CreateRdmaNetContext(const node_id_t& id, bool& exist);

  void ProcessRdmaRequest();

  // create an event to monitor the liveness of the TCP connection
  inline int CreateConnectionEvent(uint32_t fd, const node_id_t& node) {
    int ret = AE_ERR;
    if (fd > 0 && (ret = aeCreateFileEvent(el_, fd, AE_READABLE,
                                 TCPConnectionHandle, this) == AE_ERR)) {
        LOG(WARNING) << "Unrecoverable error creating cfd file event.";
    }

    assert(fdCliMap_.count(fd) == 0);
    fdCliMap_[fd] = node;
    return ret;
  }

  // delete the tcp connection event
  inline void DeleteConnectionEvent(uint32_t fd) {
    DLOG(INFO) << "Close connection " << fd;
    aeDeleteFileEvent(el_, fd, AE_READABLE);
    assert(fdCliMap_.count(fd));
    DeleteNetContext(fdCliMap_.at(fd));
    fdCliMap_.erase(fd);
  }

 private:
  void StartService(const node_id_t& id, RdmaNetResource* res);

  inline RdmaNetContext* FindContext(uint32_t qpn) {
    RdmaNetContext* ctx = nullptr;
    try {
      ctx = qpCliMap_.at(qpn);
    } catch (const std::out_of_range& oor) {
      LOG(WARNING) << "cannot find the client for qpn "
          << qpn << "(" << oor.what() << ")";
    }
    return ctx;
  }

  inline RdmaNetContext* FindContextID(const node_id_t& id) const {
    RdmaNetContext* ctx = nullptr;
    if (netmap_.count(id)) {
      ctx = dynamic_cast<RdmaNetContext*>(netmap_.at(id));
    }
    return ctx;
  }

  static void CbHandler(RdmaNet* ctx, const node_id_t& source,
                        const void* msg, size_t size,
                        RdmaNetResource* resource, uint64_t wr_id);
  RdmaNetResource* resource_ = nullptr;
  aeEventLoop* el_ = nullptr;
  int sockfd_ = 0;
  HashTable<uint32_t, RdmaNetContext*> qpCliMap_;
  // the map between file descriptor and the NetContext
  HashTable<uint32_t, node_id_t> fdCliMap_;
  // HashTable<std::string, RdmaNetContext*> idCliMap_;
  std::thread* st_ = nullptr;
  std::mutex net_lock_;
  int nthreads_;

  boost::asio::io_service ioService_;
  boost::thread_group threadpool_;
  boost::asio::io_service::work work_;
};

class RdmaNetResourceFactory:
    public Singleton<RdmaNetResourceFactory>, private Noncopyable {
  friend Singleton<RdmaNetResourceFactory>;
 public:
  ~RdmaNetResourceFactory() {
    for (RdmaNetResource* r : resources) delete r;
  }

  RdmaNetResource* getRdmaNetResource(const char* devName = nullptr);
  RdmaNetResource* newRdmaNetResource(const char* devName = nullptr);

 private:
  RdmaNetResourceFactory() {}

  std::vector<RdmaNetResource *> resources;
  const char *defaultDevname = nullptr;
};

}  // namespace ustore

#endif

#endif  // USTORE_NET_RDMA_NET_H_
