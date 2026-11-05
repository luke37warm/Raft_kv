#include "raft/network.h"

#include <cassert>

namespace raft {

Network::Network(Simulator& sim, int num_nodes)
    : sim_(sim),
      handlers_(num_nodes),
      up_(num_nodes, true),
      group_(num_nodes, 0) {}

void Network::attach(NodeId id, DeliverFn fn) { handlers_[id] = std::move(fn); }

void Network::set_delay(Time min, Time max) {
  assert(min <= max);
  delay_min_ = min;
  delay_max_ = max;
}

void Network::partition(const std::vector<std::vector<NodeId>>& groups) {
  for (int i = 0; i < size(); ++i) group_[i] = -(i + 1);  // isolated by default
  int g = 0;
  for (const auto& members : groups) {
    for (NodeId n : members) group_[n] = g;
    ++g;
  }
}

void Network::heal() {
  for (int i = 0; i < size(); ++i) group_[i] = 0;
}

bool Network::can_reach(NodeId from, NodeId to) const {
  return group_[from] == group_[to];
}

bool Network::roll(double p) {
  if (p <= 0.0) return false;
  if (p >= 1.0) return true;
  return std::uniform_real_distribution<double>(0.0, 1.0)(sim_.rng()) < p;
}

Time Network::pick_delay() {
  return std::uniform_int_distribution<Time>(delay_min_, delay_max_)(sim_.rng());
}

void Network::send(NodeId from, NodeId to, Message msg) {
  ++stats_.sent;
  if (!up_[from] || !can_reach(from, to) || roll(drop_rate_)) {
    ++stats_.dropped;
    return;
  }
  Envelope env{from, to, std::move(msg)};
  bool duplicate = roll(dup_rate_);
  if (duplicate) deliver_later(env);
  deliver_later(std::move(env));
}

void Network::deliver_later(Envelope env) {
  sim_.schedule(pick_delay(), [this, env = std::move(env)]() {
    if (!up_[env.to] || !can_reach(env.from, env.to) || !handlers_[env.to]) {
      ++stats_.dropped;
      return;
    }
    ++stats_.delivered;
    handlers_[env.to](env);
  });
}

}  // namespace raft
