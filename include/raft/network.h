#pragma once
// Simulated network: random delay, drops, duplication, partitions, and
// nodes that are "down". Reachability is checked both when a message is sent
// and when it would be delivered, so a partition (or crash) that happens while
// a message is in flight swallows it, like a real network.

#include <functional>
#include <vector>

#include "raft/sim.h"
#include "raft/types.h"

namespace raft {

class Network {
 public:
  using DeliverFn = std::function<void(const Envelope&)>;

  struct Stats {
    std::uint64_t sent = 0;
    std::uint64_t delivered = 0;
    std::uint64_t dropped = 0;
  };

  Network(Simulator& sim, int num_nodes);

  // Register the function called when a message reaches node `id`.
  void attach(NodeId id, DeliverFn fn);

  void send(NodeId from, NodeId to, Message msg);

  // One-way latency is uniform in [min, max] virtual microseconds.
  void set_delay(Time min, Time max);
  void set_drop_rate(double p) { drop_rate_ = p; }
  void set_duplicate_rate(double p) { dup_rate_ = p; }

  // Nodes can talk only if they share a group. Unlisted nodes are isolated.
  void partition(const std::vector<std::vector<NodeId>>& groups);
  void heal();

  // A down node neither sends nor receives.
  void set_node_up(NodeId id, bool up) { up_[id] = up; }
  bool node_up(NodeId id) const { return up_[id]; }

  int size() const { return static_cast<int>(up_.size()); }
  const Stats& stats() const { return stats_; }

 private:
  bool can_reach(NodeId from, NodeId to) const;
  bool roll(double p);
  Time pick_delay();
  void deliver_later(Envelope env);

  Simulator& sim_;
  std::vector<DeliverFn> handlers_;
  std::vector<bool> up_;
  std::vector<int> group_;
  Time delay_min_ = 1000;   // 1 ms
  Time delay_max_ = 5000;   // 5 ms
  double drop_rate_ = 0.0;
  double dup_rate_ = 0.0;
  Stats stats_;
};

}  // namespace raft
