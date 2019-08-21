// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_CONSOLE_H_
#define USTORE_CLI_CONSOLE_H_

#include <list>
#include <string>
#include <unordered_set>
#include <vector>
#include "cli/command.h"

namespace ustore {
namespace cli {

#define CONSOLE_CMD_HANDLER(cmd, handler) do { \
  CMD_HANDLER(cmd, handler); \
  console_commands_.insert(cmd); \
} while(0)

#define CONSOLE_CMD_ALIAS(cmd, alias) do { \
  CMD_ALIAS(cmd, alias); \
  console_commands_.insert(alias); \
} while(0)

class Console : public Command {
 public:
  explicit Console(DB* db) noexcept;
  ~Console() = default;

  ErrorCode Run(int argc, char* argv[]) = delete;
  ErrorCode Run();

 protected:
  void PrintHelp() override;
  void PrintConsoleCommandHelp(std::ostream& os = std::cout);

 private:
  void Run(const std::string& cmd_line);
  bool ReplaceWithHistory(std::string& cmd_line);
  void MarkCurrentCommandLineToComment();

  ErrorCode ExecHistory();
  ErrorCode ExecDumpHistory();

  std::unordered_set<std::string> console_commands_;
  std::vector<std::string> history_;
  std::list<size_t> comment_history_lines;
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_CONSOLE_H_
