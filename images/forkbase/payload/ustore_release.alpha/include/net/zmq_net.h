// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NET_ZMQ_NET_H_
#define USTORE_NET_ZMQ_NET_H_

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "net/net.h"

namespace ustore {

// Common network interface, extended by Client and Server
class ZmqNet : public Net {
 public:
  explicit ZmqNet(const node_id_t& id, int nthreads = 1) :
      Net(id), nthreads_(nthreads) {}
  ~ZmqNet() = default;

  NetContext* CreateNetContext(const node_id_t& id) override;
  virtual void Start() {}
  void Stop() override;
  // process the received msg
  void Dispatch(const node_id_t& source, const void *msg, int size);

  std::string request_ep_, inproc_ep_;
 protected:
  void *recv_sock_, *backend_sock_;  // router and backend socket
  void *request_sock_;  // inproc router socket for multi-threaded client
  int nthreads_;  // number of processing threads
  std::vector<std::thread> backend_threads_;
};

/**
 * Client side of the network. One DEALER socket per connection to each
 * worker. The main thread polls these sockets and forwards messages to
 * backend socket. A number of ClientThread connects to the backend socket
 * and processes the response.
 */
class ClientZmqNet : public ZmqNet {
 public:
  explicit ClientZmqNet(int nthreads = 1);
  ~ClientZmqNet() = default;

  void Start() override;
  // receive thread
  void ClientThread();
  std::atomic<int> request_counter_, timeout_counter_;
};

/**
 * Server side of the network. One ROUTER socket to accept and receive all
 * client messages. Processing threads are embedded in ServerZmqNetContext,
 * each connects to a backend socket (at inproc_ep_) to receive requests.
 * Reponses from these threads are written to another backend socket
 * (result_ep_).
 * The main thread polls the ROUTER socket and result_ep_ socket to forward
 * messages to the backend and to the client respectively.
 */
class ServerZmqNet : public ZmqNet {
 public:
  explicit ServerZmqNet(const node_id_t& id, int nthreads = 1);
  ~ServerZmqNet() = default;

  void Start() override;
  std::string result_ep_;  // ipc endpoint for sending results

 private:
  void *result_sock_;
};

// Client side's network context. Each contains a connection to a
// remote worker
class ZmqNetContext : public NetContext {
 public:
  ZmqNetContext(const node_id_t& src, const node_id_t& dest, ClientZmqNet *net);
  ~ZmqNetContext();

  ssize_t Send(const void* ptr, size_t len, CallBack* func = nullptr) override;
  void *GetSocket() { return send_sock_; }

 protected:
  std::mutex send_lock_;
  void *send_sock_;
  ClientZmqNet *client_net_;
};

// Server side's network context. Contains a processing thread, a connection
// to the backend socket, and another connection to the result socket.
class ServerZmqNetContext : public NetContext {
 public:
  ServerZmqNetContext(const node_id_t& src, const node_id_t& dest,
          const string& ipc_ep, const string& result_ep, const string& id);
  ~ServerZmqNetContext() = default;

  ssize_t Send(const void* ptr, size_t len, CallBack* func = nullptr) override;

  // processing thread
  void Start(ServerZmqNet *net);
 private:
  std::mutex send_lock_;
  void *send_sock_, *recv_sock_, *client_id_ = nullptr;
  string id_;
};

}  // namespace ustore

#endif  // USTORE_NET_ZMQ_NET_H_
