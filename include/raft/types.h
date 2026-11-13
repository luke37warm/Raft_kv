#pragma once
// Core vocabulary shared by every component: ids, log entries, RPC messages.

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace raft {

using NodeId = int;        // 0 .. n-1
using Term = std::uint64_t;
using Index = std::uint64_t;  // log indices are 1-based; 0 means "no entry"
using Time = std::uint64_t;   // virtual microseconds

struct LogEntry {
  Term term = 0;
  std::string command;  // opaque to Raft; the KV layer encodes PUT/DELETE here
};

// --- RPCs (Raft paper, Figure 2) -------------------------------------------

struct RequestVote {
  Term term = 0;
  NodeId candidate = -1;
  Index last_log_index = 0;
  Term last_log_term = 0;
};

struct RequestVoteReply {
  Term term = 0;
  bool granted = false;
};

struct AppendEntries {
  Term term = 0;
  NodeId leader = -1;
  Index prev_log_index = 0;
  Term prev_log_term = 0;
  std::vector<LogEntry> entries;  // empty => heartbeat
  Index leader_commit = 0;
};

struct AppendEntriesReply {
  Term term = 0;
  bool success = false;
  Index match_index = 0;  // valid when success; lets the leader advance fast
  Index conflict_index = 0;  // valid when !success; lets the leader back up fast
};

using Message =
    std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply>;

struct Envelope {
  NodeId from = -1;
  NodeId to = -1;
  Message msg;
};

// The term carried by any RPC. Every handler needs this for the
// "if term > currentTerm, step down" rule.
inline Term message_term(const Message& m) {
  return std::visit([](const auto& x) { return x.term; }, m);
}

}  // namespace raft
