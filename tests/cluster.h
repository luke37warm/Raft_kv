#pragma once
// Test harness: a whole Raft cluster on the simulator, plus checkers for the
// safety properties from the Raft paper (Figure 3). Any violation is recorded
// as a message in `violations` so tests can assert the list is empty.

#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "raft/network.h"
#include "raft/node.h"
#include "raft/sim.h"
#include "raft/storage.h"

namespace raft {

constexpr Time kMs = 1000;
constexpr Time kSec = 1000 * kMs;

struct Cluster {
  Simulator sim;
  Network net;
  int n;
  std::vector<std::unique_ptr<MemoryStorage>> storage;
  std::vector<std::unique_ptr<Node>> nodes;

  // What each node's state machine has applied since its last (re)start.
  std::vector<std::vector<std::pair<Index, std::string>>> applied;

  std::map<Term, std::set<NodeId>> leaders_by_term;
  std::map<Index, LogEntry> committed;  // every entry ever observed committed
  std::vector<std::string> violations;

  Cluster(std::uint64_t seed, int num) : sim(seed), net(sim, num), n(num), applied(num) {
    for (int i = 0; i < n; ++i) {
      std::vector<NodeId> peers;
      for (int j = 0; j < n; ++j)
        if (j != i) peers.push_back(j);
      storage.push_back(std::make_unique<MemoryStorage>());
      nodes.push_back(std::make_unique<Node>(i, peers, sim, net, *storage[i]));

      nodes[i]->on_become_leader = [this](NodeId id, Term t) {
        leaders_by_term[t].insert(id);
        check_leader_completeness(id);
      };
      nodes[i]->on_apply = [this, i](Index idx, const LogEntry& e) {
        if (!applied[i].empty() && idx <= applied[i].back().first)
          violate("node " + std::to_string(i) + " applied index " +
                  std::to_string(idx) + " out of order");
        applied[i].emplace_back(idx, e.command);
      };
      net.attach(i, [this, i](const Envelope& e) { nodes[i]->on_message(e); });
    }
    for (auto& nd : nodes) nd->start();
  }

  void violate(std::string msg) { violations.push_back(std::move(msg)); }

  void crash(int i) {
    nodes[i]->crash();
    net.set_node_up(i, false);
    applied[i].clear();  // an in-memory state machine dies with the process
  }
  void restart(int i) {
    net.set_node_up(i, true);
    nodes[i]->restart();
  }

  std::vector<NodeId> leaders() const {
    std::vector<NodeId> out;
    for (int i = 0; i < n; ++i)
      if (nodes[i]->running() && nodes[i]->role() == Role::Leader) out.push_back(i);
    return out;
  }

  // The single leader, or -1 if there are zero or several.
  NodeId leader() const {
    auto ls = leaders();
    return ls.size() == 1 ? ls[0] : -1;
  }

  // Propose to the leader. Returns the log index, or 0 if there is no unique leader.
  Index propose(const std::string& cmd) {
    NodeId l = leader();
    return l < 0 ? 0 : nodes[l]->propose(cmd);
  }

  // Advance time in small steps, checking invariants as we go, so a transient
  // violation cannot hide between two coarse checks.
  void run(Time duration, Time step = 20 * kMs) {
    for (Time t = 0; t < duration; t += step) {
      sim.run_for(step);
      check_invariants();
    }
  }

  bool election_safe() const {
    for (const auto& [term, ids] : leaders_by_term)
      if (ids.size() > 1) return false;
    return true;
  }

  // State Machine Safety, in its strongest form: once any node has seen an
  // entry committed at index i, every node's committed entry at i is the same.
  void check_invariants() {
    for (int i = 0; i < n; ++i) {
      const auto& log = nodes[i]->log();
      for (Index idx = 1; idx <= nodes[i]->commit_index(); ++idx) {
        if (idx > log.size()) {
          violate("node " + std::to_string(i) + " commit_index beyond its log");
          break;
        }
        const LogEntry& e = log[idx - 1];
        auto it = committed.find(idx);
        if (it == committed.end()) {
          committed.emplace(idx, e);
        } else if (it->second.term != e.term || it->second.command != e.command) {
          violate("committed entry at index " + std::to_string(idx) +
                  " differs on node " + std::to_string(i));
        }
      }
    }
  }

  // Leader Completeness: a new leader's log must already contain every entry
  // that was ever committed.
  void check_leader_completeness(NodeId id) {
    const auto& log = nodes[id]->log();
    for (const auto& [idx, e] : committed) {
      if (idx > log.size() || log[idx - 1].term != e.term ||
          log[idx - 1].command != e.command) {
        violate("leader " + std::to_string(id) + " is missing committed index " +
                std::to_string(idx));
        return;
      }
    }
  }

  // Log Matching: if two logs hold an entry with the same index and term, the
  // logs are identical up to that index. Checked over every pair of nodes.
  void check_log_matching() {
    for (int a = 0; a < n; ++a) {
      for (int b = a + 1; b < n; ++b) {
        const auto& la = nodes[a]->log();
        const auto& lb = nodes[b]->log();
        std::size_t m = std::min(la.size(), lb.size());
        for (std::size_t i = m; i-- > 0;) {
          if (la[i].term != lb[i].term) continue;
          for (std::size_t j = 0; j <= i; ++j) {
            if (la[j].term != lb[j].term || la[j].command != lb[j].command) {
              violate("log matching broken between nodes " + std::to_string(a) +
                      " and " + std::to_string(b));
              return;
            }
          }
          break;  // the highest shared (index, term) covers everything below it
        }
      }
    }
  }

  bool logs_identical() const {
    for (int i = 1; i < n; ++i) {
      const auto& a = nodes[0]->log();
      const auto& b = nodes[i]->log();
      if (a.size() != b.size()) return false;
      for (std::size_t j = 0; j < a.size(); ++j)
        if (a[j].term != b[j].term || a[j].command != b[j].command) return false;
    }
    return true;
  }

  std::vector<std::string> applied_commands(int i) const {
    std::vector<std::string> out;
    for (const auto& [idx, cmd] : applied[i]) out.push_back(cmd);
    return out;
  }

  // True if every node applied exactly the same commands in the same order.
  bool applied_identical() const {
    for (int i = 1; i < n; ++i)
      if (applied_commands(i) != applied_commands(0)) return false;
    return true;
  }

  void print_violations() const {
    for (const auto& v : violations) std::fprintf(stderr, "  VIOLATION: %s\n", v.c_str());
  }
};

}  // namespace raft