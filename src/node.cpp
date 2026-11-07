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
  leader_id_=-1;
  voters_.clear();
  reset_election_timer();
}

void Node::crash() {
  running_ = false;
  cancel_timers();
  role_ = Role::Follower;
  commit_index_ = 0;
  last_applied_ = 0;
  leader_id_=-1;
  voters_.clear();
  // term_/voted_for_/log_ in memory are now stale; start() reloads them.
}

void Node::persist() { storage_.save(PersistentState{term_, voted_for_, log_}); }

void Node::cancel_timers() {
  if (election_timer_) {
    sim_.cancel(*election_timer_);
    election_timer_.reset();
  }
  if (heartbeat_timer_) {
    sim_.cancel(*heartbeat_timer_);
    heartbeat_timer_.reset();
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
  if (role_ == Role::Leader) return;
  start_election();
}

bool Node::observe_term(Term t) {
  if (t <= term_) return false;
  bool was_leader = (role_ == Role::Leader);
  term_ = t;
  voted_for_ = -1;
  role_ = Role::Follower;
  leader_id_ = -1;
  persist();
  if (was_leader) {
    if (heartbeat_timer_) {
      sim_.cancel(*heartbeat_timer_);
      heartbeat_timer_.reset();
    }
    reset_election_timer();
  }
  return true;
}

// --- Elections ---

void Node::start_election() {
  ++term_;
  role_ = Role::Candidate;
  voted_for_ = id_;
  leader_id_ = -1;
  voters_ = {id_};
  persist();
  reset_election_timer();

  if (static_cast<int>(voters_.size()) * 2 > cluster_size()) {
    become_leader();
    return;
  }
  RequestVote rv{term_, id_, last_log_index(), last_log_term()};
  for (NodeId p : peers_) net_.send(id_, p, rv);
}

void Node::become_leader() {
  role_ = Role::Leader;
  leader_id_ = id_;
  if (election_timer_) {
    sim_.cancel(*election_timer_);
    election_timer_.reset();
  }
  if (on_become_leader) on_become_leader(id_, term_);
  send_heartbeats();
}

void Node::send_heartbeats() {
  if (role_ != Role::Leader) return;
  AppendEntries ae{term_, id_, last_log_index(), last_log_term(), {}, commit_index_};
  for (NodeId p : peers_) net_.send(id_, p, ae);
  heartbeat_timer_ = sim_.schedule(cfg_.heartbeat_interval, [this]() {
    heartbeat_timer_.reset();
    send_heartbeats();
  });
}

bool Node::candidate_log_up_to_date(const RequestVote& rv) const {
  if (rv.last_log_term != last_log_term()) return rv.last_log_term > last_log_term();
  return rv.last_log_index >= last_log_index();
}

// --- RPC handlers ---

void Node::handle_request_vote(NodeId from, const RequestVote& rv) {
  bool grant = false;
  if (rv.term == term_ && (voted_for_ == -1 || voted_for_ == rv.candidate) &&
      candidate_log_up_to_date(rv)) {
    grant = true;
    voted_for_ = rv.candidate;
    persist();
    reset_election_timer();
  }
  net_.send(id_, from, RequestVoteReply{term_, grant});
}

void Node::handle_request_vote_reply(NodeId from, const RequestVoteReply& r) {
  if (role_ != Role::Candidate || r.term != term_ || !r.granted) return;
  voters_.insert(from);
  if (static_cast<int>(voters_.size()) * 2 > cluster_size()) become_leader();
}

void Node::handle_append_entries(NodeId from, const AppendEntries& ae) {
  if (ae.term < term_) {
    net_.send(id_, from, AppendEntriesReply{term_, false, 0});
    return;
  }
  role_ = Role::Follower;
  leader_id_ = ae.leader;
  reset_election_timer();
  net_.send(id_, from, AppendEntriesReply{term_, true, 0});
}

void Node::on_message(const Envelope& env) {
  if (!running_) return;
  observe_term(message_term(env.msg));
  if (const auto* rv = std::get_if<RequestVote>(&env.msg)) {
    handle_request_vote(env.from, *rv);
  } else if (const auto* rvr = std::get_if<RequestVoteReply>(&env.msg)) {
    handle_request_vote_reply(env.from, *rvr);
  } else if (const auto* ae = std::get_if<AppendEntries>(&env.msg)) {
    handle_append_entries(env.from, *ae);
  }
}

}  // namespace raft