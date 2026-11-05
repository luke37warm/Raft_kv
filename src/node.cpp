#include "raft/node.h"

#include <random>

namespace raft {

Node::Node(NodeId id, std::vector<NodeId> peers, Simulator& sim, Network& net,
           Storage& storage, NodeConfig cfg)
    : id_(id),
      peers_(std::move(peers)),
      sim_(sim),
      net_(net),
      storage_(storage),
      cfg_(cfg) {}

void Node::start() {
  cancel_timers();
  PersistentState st = storage_.load();
  term_ = st.current_term;
  voted_for_ = st.voted_for;
  log_ = std::move(st.log);

  running_ = true;
  role_ = Role::Follower;
  commit_index_ = 0;
  last_applied_ = 0;
  reset_election_timer();
}

void Node::crash() {
  running_ = false;
  cancel_timers();
  role_ = Role::Follower;
  commit_index_ = 0;
  last_applied_ = 0;
  // term_/voted_for_/log_ in memory are now stale; start() reloads them.
}

void Node::persist() { storage_.save(PersistentState{term_, voted_for_, log_}); }

void Node::cancel_timers() {
  if (election_timer_) {
    sim_.cancel(*election_timer_);
    election_timer_.reset();
  }
}

void Node::reset_election_timer() {
  if (election_timer_) sim_.cancel(*election_timer_);
  Time timeout = std::uniform_int_distribution<Time>(
      cfg_.election_timeout_min, cfg_.election_timeout_max)(sim_.rng());
  election_timer_ = sim_.schedule(timeout, [this]() {
    election_timer_.reset();
    ++election_timeouts_;
    on_election_timeout();
  });
}

void Node::on_election_timeout() {
  // Milestone 2 replaces this with: become candidate, bump term, vote for
  // self, send RequestVote to every peer. For now, just keep the timer alive.
  reset_election_timer();
}

bool Node::observe_term(Term t) {
  if (t <= term_) return false;
  term_ = t;
  voted_for_ = -1;
  role_ = Role::Follower;
  persist();  // must hit storage before we reply to anyone
  return true;
}

void Node::on_message(const Envelope& env) {
  if (!running_) return;
  observe_term(message_term(env.msg));
  // Per-RPC handling lands in milestones 2 and 3.
}

}  // namespace raft
