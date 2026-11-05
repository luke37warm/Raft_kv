#pragma once
// A single Raft server. Milestone 1 builds the skeleton: persistent state,
// crash/restart semantics, the randomized election timer, and the universal
// "higher term => step down" rule. Elections arrive in milestone 2 and log
// replication in milestone 3.

#include <optional>
#include <vector>

#include "raft/network.h"
#include "raft/sim.h"
#include "raft/storage.h"
#include "raft/types.h"

namespace raft {

enum class Role { Follower, Candidate, Leader };

struct NodeConfig {
  Time election_timeout_min = 150000;  // 150 ms
  Time election_timeout_max = 300000;  // 300 ms
  Time heartbeat_interval = 50000;     // 50 ms
};

class Node {
 public:
  Node(NodeId id, std::vector<NodeId> peers, Simulator& sim, Network& net,
       Storage& storage, NodeConfig cfg = NodeConfig());

  // Load persistent state from storage, become a follower, arm the timer.
  void start();
  // Simulate a process crash: lose all volatile state and timers. Storage is
  // untouched.
  void crash();
  void restart() { start(); }

  void on_message(const Envelope& env);

  NodeId id() const { return id_; }
  bool running() const { return running_; }
  Role role() const { return role_; }
  Term term() const { return term_; }
  NodeId voted_for() const { return voted_for_; }
  Index commit_index() const { return commit_index_; }
  Index last_log_index() const { return log_.size(); }
  std::uint64_t election_timeouts() const { return election_timeouts_; }

 private:
  void persist();
  void reset_election_timer();
  void cancel_timers();
  void on_election_timeout();
  // Returns true if the term moved forward (and we stepped down).
  bool observe_term(Term t);

  NodeId id_;
  std::vector<NodeId> peers_;
  Simulator& sim_;
  Network& net_;
  Storage& storage_;
  NodeConfig cfg_;

  bool running_ = false;

  // Persistent (mirrored to storage_)
  Term term_ = 0;
  NodeId voted_for_ = -1;
  std::vector<LogEntry> log_;  // log_[i] holds index i+1

  // Volatile
  Role role_ = Role::Follower;
  Index commit_index_ = 0;
  Index last_applied_ = 0;

  std::optional<Simulator::TimerId> election_timer_;
  std::uint64_t election_timeouts_ = 0;
};

}  // namespace raft
