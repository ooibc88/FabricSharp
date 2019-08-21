// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_ACCESS_LOGGING_H_
#define USTORE_CLUSTER_ACCESS_LOGGING_H_

#include <boost/filesystem.hpp>
#include <cstdio>
#include <ctime>
#include <string>
#include "hash/hash.h"
#include "utils/env.h"

namespace ustore {

namespace fs = boost::filesystem;

/*
 * Access Log is for recording client request history.
 * It is only enabled when dir != "".
 * When it is enabled, we will get a log file in following format:
 *
 *   [time],[operation],[key],[branch],[resulting version]
 *
 * time and operation fields are compulsory, while others might be empty
 */
class AccessLogging {
 public:
  AccessLogging(const std::string& dir, const std::string& addr) {
    // create access log dir
    if (dir != "") {
      // create access log dir
      fs::create_directory(fs::path(dir));
      auto pattern = Env::Instance()->config().data_file_pattern();
      auto log_file = dir + "/" + pattern + "_" + addr + ".access";
      // replace host:ip format with host-ip format
      std::replace(log_file.begin(), log_file.end(), ':', '-');
      file_ = fopen(log_file.c_str(), "a");
    }
  }
  ~AccessLogging() { if (file_) fclose(file_); }

  void Append(const char* op, const std::string& key, const std::string& branch,
              const std::string& version) {
    if (file_) {
      // TODO(wangsh): in order to support dataset management, we ignore all
      //    keys in XX::XX format in an ad-hoc way
      if (key.find("::") != std::string::npos) return;
      time_t rw_time = time(nullptr);
      struct tm tm_time;
      localtime_r(&rw_time, &tm_time);
      fprintf(file_, "[d%02d%02d t%02d:%02d:%02d] %s,%s,%s,%s\n",
              1 + tm_time.tm_mon, tm_time.tm_mday,
              tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec,
              op, key.c_str(), branch.c_str(), version.c_str());
      fflush(file_);
    }
  }

 private:
  FILE* file_ = nullptr;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_ACCESS_LOGGING_H_
