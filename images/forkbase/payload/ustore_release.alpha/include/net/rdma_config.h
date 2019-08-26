// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NET_RDMA_CONFIG_H_
#define USTORE_NET_RDMA_CONFIG_H_

#ifdef USE_RDMA

#include <vector>
#include <string>


namespace ustore {

using std::string;
using std::stringstream;
using std::vector;

#define MAX_CQ_EVENTS 1024

#define MIN_RESERVED_FDS 32
#define EVENTLOOP_FDSET_INCR (MIN_RESERVED_FDS+96)
#define EVENTLOOP_FDSET_INCR (MIN_RESERVED_FDS+96)

#define TCP_BACKLOG       511     /* TCP listen backlog */
#define IP_STR_LEN 46  // INET6_ADDRSTRLEN is 46, but we need to be sure

#define MAX_NUM_WORKER 20
#define MAX_MASTER_PENDING_MSG 32
#define MAX_UNSIGNALED_MSG 32

#define HW_MAX_PENDING 16351
#define MAX_WORKER_PENDING_MSG 1024
// shouldn't larger than HW_MAX_PENDING
#define MAX_WORKER_RECV_MSG MAX_WORKER_PENDING_MSG
// shouldn't larger than HW_MAX_PENDING
#define MAX_MASTER_RECV_MSG MAX_MASTER_PENDING_MSG
#define MASTER_RDMA_SRQ_RX_DEPTH \
    (MAX_MASTER_RECV_MSG * MAX_NUM_WORKER)
#define WORKER_RDMA_SRQ_RX_DEPTH \
    (MAX_WORKER_RECV_MSG * (MAX_NUM_WORKER-1) + MAX_MASTER_RECV_MSG)

#define MAX_REQUEST_SIZE 10240
#define WORKER_BUFFER_SIZE (MAX_WORKER_PENDING_MSG * MAX_REQUEST_SIZE)
#define MASTER_BUFFER_SIZE (MAX_MASTER_PENDING_MSG * MAX_REQUEST_SIZE)

#define MAX_IPPORT_STRLEN 21  // 192.168.154.154:12345
// 192.168.154.154:12345,
#define MAX_WORKERS_STRLEN (MAX_NUM_WORKER*MAX_IPPORT_STRLEN+MAX_NUM_WORKER-1)

#define INIT_WORKQ_SIZE 2000

#define RDMA_RESOURCE_EXCEPTION 1
#define RDMA_CONTEXT_EXCEPTION 2
#define SERVER_NOT_EXIST_EXCEPTION 3
#define SERVER_ALREADY_EXIST_EXCEPTION 4

  /** 4 bytes for lid + 8 bytes qpn
   * + 8 bytes for psn
   * seperated by 2 colons */
#define MASTER_RDMA_CONN_STRLEN 22
#define WORKER_RDMA_CONN_STRLEN (MASTER_RDMA_CONN_STRLEN + 8 + 16 + 2)
// 4: four-digital wid, 1: ':', 1: \0
#define MAX_CONN_STRLEN (WORKER_RDMA_CONN_STRLEN+4+1)
// 192.148.111.111:11234; // hostname:port
#define MAX_CONN_STRLEN_G WORKER_RDMA_CONN_STRLEN + 100

#define HALF_BITS 0xffffffff
#define QUARTER_BITS 0xffff

#define RECV_SLOT_STEP 100

struct RdmaRequest {
  ibv_wr_opcode op;
  const void* src;
  size_t len;
  unsigned int id;
  bool signaled;
  void* dest;
  uint32_t imm;
  uint64_t oldval;
  uint64_t newval;
};

#ifndef roundup
# define roundup(x, y)  ((((x) + ((y) - 1)) / (y)) * (y))
#endif /* !defined(roundup) */

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x)   __builtin_expect(!!(x), 0)

#define RECV_SLOT_STEP 100
#define BPOS(s) (s/RECV_SLOT_STEP)
#define BOFF(s) ((s%RECV_SLOT_STEP)*MAX_REQUEST_SIZE)
#define RMINUS(a, b, rz) ((a) >= (b) ? (a) - (b) : (a) + (rz) - (b))


#define DEFAULT_SPLIT_CHAR ':'
template <class T>
inline vector<T>& Split(stringstream& ss, vector<T>& elems, char delim) {
  T item;
  while (ss >> item) {
    elems.push_back(item);
    if (ss.peek() == delim) ss.ignore();
  }
  return elems;
}

template <>
vector<string>& Split<string>(stringstream& ss,
                              vector<string>& elems, char delim);

template <class T>
inline vector<T>& Split(const string& s, vector<T>& elems,
                        char delim = DEFAULT_SPLIT_CHAR) {
  stringstream ss(s);
  return Split(ss, elems, delim);
}

template <class T>
inline vector<T>& Split(char *s, vector<T>& elems,
                        char delim = DEFAULT_SPLIT_CHAR) {
  stringstream ss(s);
  return Split(ss, elems, delim);
}

// raddr means memory addr that is registered with RDMA device
typedef void* raddr;

}  // end namespace ustore

#endif
#endif  // USTORE_NET_RDMA_CONFIG_H_
