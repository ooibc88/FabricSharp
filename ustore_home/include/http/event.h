// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_EVENT_H_
#define USTORE_HTTP_EVENT_H_

#include <time.h>
#include <vector>

namespace ustore {

constexpr int kNone = 0;
constexpr int kReadable  = 1;  // EPOLLIN
constexpr int kWritable  = 2;  // EPOLLOUT | EPOLLERR | EPOLLHUP
constexpr int kEdege = 4;

constexpr int kDefaultTimeout = 10;  // or 0: NON_BLOCKING, -1: BLOCKING
constexpr int kDefaultMaxFd = 10000;

struct EventLoop;

// Types and data structures
typedef void FileProc(struct EventLoop *event_loop, int fd, void *client_data,
                      int mask);

// File event structure
struct FileEvent {
  int mask;  // one of AE_(kReadable|kWritable)
  FileProc *rfile_proc;
  FileProc *wfile_proc;
  void *client_data;
};

// A fired event
struct FiredEvent {
  int fd;
  int mask;
};

struct EpollState {
  int epfd;
  struct epoll_event *events;
};

/*
 * event loop wrapper of epoll for easy use
 */
class EventLoop {
 public:
  explicit EventLoop(int setsize = kDefaultMaxFd);
  ~EventLoop();

  void Start();
  void Stop();

  int CreateFileEvent(int fd, int mask, FileProc *proc, void *client_data);
  void DeleteFileEvent(int fd, int mask);
  int GetFileEvents(int fd);

 private:
  int ResizeSetSize(int setsize);

  // return number of processed events
  int ProcessEvents(int timeout = kDefaultTimeout);
  int EpollAddEvent(int fd, int mask);
  void EpollDelEvent(int fd, int delmask);
  int EpollPoll(int timeout = kDefaultTimeout);

  int maxfd_;  // highest file descriptor currently registered
  int setsize_;  // max number of file descriptors tracked
  std::vector<FileEvent> events_;  // Registered events
  std::vector<FiredEvent> fired_;  // Fired events
  volatile int stop_;
  EpollState* estate_;
};
}  // namespace ustore

#endif  // USTORE_HTTP_EVENT_H_
