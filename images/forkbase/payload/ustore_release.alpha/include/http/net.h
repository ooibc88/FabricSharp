// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_NET_H_
#define USTORE_HTTP_NET_H_

#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <sstream>
#include "http/event.h"
#include "http/settings.h"

namespace ustore {

#define TCP_BACKLOG 511

#define ST_SUCCESS 0
#define ST_ERROR -1
#define ST_CLOSED 1
#define ST_INPROCESS 2

/*
 * Net socket class based on Linux TCP socket
 */
class Socket {
 public:
  explicit Socket(int fd) : fd_(fd) {}
  Socket(const std::string& ip, int port) : ip_(ip), port_(port) {}
  Socket(const std::string& ip, int port, int fd)
      : ip_(ip), port_(port), fd_(fd) {}
  ~Socket() { if (fd_ > 0) close(fd_); }

  // get the file descriptor of the socket
  inline int GetFD() const noexcept { return fd_; }

 protected:
  std::string ip_;
  int port_ = 0;
  int fd_ = -1;
};

class ClientSocket : public Socket {
 public:
  ClientSocket(const std::string& ip, int port) : Socket(ip, port) {}
  ClientSocket(const std::string& ip, int port, int fd)
      : Socket(ip, port, fd) {}
  ~ClientSocket() = default;

  // connect to the server
  int Connect();

  // send buf[0, size] over the socket
  int Send(const void* buf, int size);

  /*
   * read the data (max = size) from socket and put the data in buf
   * return: size of data received
   */
  int Recv(void* buf, int size = kDefaultRecvSize);
  /*
   * read the data (max = size) from socket and put the data in a std::string and return
   * return: received data as a std::string
   */
  std::string Recv(int size = kDefaultRecvSize);
  /*
   * read the data (max = size) from socket and write the data to std::stringstream
   * return: size of data received
   */
  int Recv(std::stringstream& ss, int size = kDefaultRecvSize);
};

class ServerSocket : public Socket {
 public:
  explicit ServerSocket(int port, const std::string& bind_addr = "",
                        int backlog = TCP_BACKLOG)
      : Socket(bind_addr, port), backlog_(backlog) {}
  ~ServerSocket() = default;

  /*
   * start listen to the socket
   * return:
   * if failed, return ST_ERROR
   * otherwise, return ST_SUCCESS
   */
  int Listen();

  /*
   * accept a client socket
   * return:
   * if failed, return nullptr
   * otherwise, return the newly created ClientSocket
   */
  ClientSocket* Accept();

 private:
  int backlog_;
};
}  // namespace ustore

#endif  // USTORE_HTTP_NET_H_
