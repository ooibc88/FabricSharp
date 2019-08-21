// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_SERVER_H_
#define USTORE_HTTP_SERVER_H_

#include <thread>
#include <string>
#include <unordered_map>
#include "spec/db.h"
#include "spec/object_db.h"
#include "http/net.h"

namespace ustore {

/*
 * Http Server class
 */
class HttpServer {
 public:
  explicit HttpServer(DB* db, int port,
                      const string& bind_addr = "", int backlog = TCP_BACKLOG);
  ~HttpServer();

  /*
   * start the server
   * @threads_num:  number of threads the server have
   *                one thread for one event loop
   */
  int Start(int threads_num = 1);

  // stop the server
  inline void Stop() {
    for (int i = 0; i < threads_num_; i++) el_[i]->Stop();
  }

//  // return the ServerSocket reference
//  inline ServerSocket& GetServerSocket() {
//    return ss_;
//  }

  // accept and put into the map
  inline ClientSocket* Accept() {
    ClientSocket* cs = ss_.Accept();
    CHECK_EQ(clients_.count(cs->GetFD()), size_t(0));
    clients_[cs->GetFD()] = cs;
    return cs;
  }

  // delete a client
  inline void Close(ClientSocket* cs) {
    clients_.erase(cs->GetFD());
    delete cs;
  }

  // delete a client
  inline void Close(int fd) {
    CHECK(clients_.count(fd));
    delete clients_[fd];
    clients_.erase(fd);
  }

  // get the ClientSocket based on fd
  inline ClientSocket* GetClientSocket(int fd) {
    CHECK(clients_.count(fd));
    return clients_[fd];
  }

  // set the maximum size of the event loop
  // i.e., the max number of file descriptors the event loop supports
  inline void SetEventLoopSize(int size) {
    el_size_ = size;
  }

  // get the objectdb
  inline ObjectDB& GetODB() {
    return odb_;
  }

  // dispatch the Client to an eventloop
  int DispatchClientSocket(ClientSocket* cs);

 private:
  ObjectDB odb_;
  ServerSocket ss_;
  int threads_num_ = 1;
  int el_size_ = 10000;
  std::thread** ethreads_ = nullptr;
  EventLoop** el_ = nullptr;
  std::unordered_map<int, ClientSocket*> clients_;
};
}  // namespace ustore

#endif  // USTORE_HTTP_SERVER_H_
