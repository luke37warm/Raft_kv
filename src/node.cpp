#include "raft/node.h"

#include <random>
#include <algorithm>

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
  next_index_.clear();
  match_index_.clear();
}

void Node::crash() {
  running_ = false;
  cancel_timers();
  role_ = Role::Follower;
  commit_index_ = 0;
  last_applied_ = 0;
  leader_id_=-1;
  voters_.clear();
  next_index_.clear();
  match_index_.clear();
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
    for (NodeId p : peers_) {
    next_index_[p] = last_log_index() + 1;
    match_index_[p] = 0;
  }

  // Append a no-op from our own term (explained below).
  log_.push_back(LogEntry{term_, ""});
  persist();
  advance_commit_index();  // a single-node cluster commits it right away

  if (on_become_leader) on_become_leader(id_, term_);
  send_heartbeats();
}

void Node::send_heartbeats() {
  if (role_ != Role::Leader) return;
  broadcast_append();  // a heartbeat is just an AppendEntries; it also retransmits
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
    net_.send(id_, from, AppendEntriesReply{term_, false, 0, 0});
    return;
  }
  role_ = Role::Follower;
  leader_id_ = ae.leader;
  reset_election_timer();

  // Consistency check: we must hold the entry just before the new ones, with
  // the same term.
  const Index prev = ae.prev_log_index;
  if (prev > last_log_index()) {
    net_.send(id_, from, AppendEntriesReply{term_, false, 0, last_log_index() + 1});
    return;
  }
  if (prev > 0 && log_[prev - 1].term != ae.prev_log_term) {
    Term bad = log_[prev - 1].term;
    Index first = prev;
    while (first > 1 && log_[first - 2].term == bad) --first;
    net_.send(id_, from, AppendEntriesReply{term_, false, 0, first});
    return;
  }

  // Append. Delete only from the first CONFLICTING entry onwards.
  bool changed = false;
  for (std::size_t i = 0; i < ae.entries.size(); ++i) {
    Index idx = prev + 1 + i;
    if (idx <= last_log_index()) {
      if (log_[idx - 1].term == ae.entries[i].term) continue;  // already have it
      log_.resize(idx - 1);
    }
    log_.push_back(ae.entries[i]);
    changed = true;
  }
  if (changed) persist();

  // Learn the commit point, capped at the last entry this message vouches for.
  Index last_new = prev + ae.entries.size();
  if (ae.leader_commit > commit_index_) {
    Index target = std::min(ae.leader_commit, last_new);
    if (target > commit_index_) {
      commit_index_ = target;
      apply_committed();
    }
  }

  net_.send(id_, from, AppendEntriesReply{term_, true, last_new, 0});
}

void Node::handle_append_entries_reply(NodeId from, const AppendEntriesReply& r) {
  if (role_ != Role::Leader || r.term != term_) return;  // stale reply
  Index& match = match_index_[from];
  Index& next = next_index_[from];

  if (r.success) {
    if (r.match_index > match) match = r.match_index;  // never move backwards
    if (next < match + 1) next = match + 1;
    advance_commit_index();
    if (next <= last_log_index()) replicate_to(from);  // more to send
  } else {
    if (next <= 1) return;
    Index hint = r.conflict_index > 0 ? r.conflict_index : 1;
    Index candidate = std::min(next - 1, hint);
    next = std::max<Index>({1, match + 1, candidate});
    replicate_to(from);
  }
}

Index Node::propose(std::string command) {
  if (!running_ || role_ != Role::Leader || command.empty()) return 0;
  log_.push_back(LogEntry{term_, std::move(command)});
  persist();
  advance_commit_index();  // single-node cluster: we are the majority
  broadcast_append();
  return last_log_index();
}

void Node::replicate_to(NodeId peer) {
  Index next = next_index_[peer];
  Index prev = next - 1;
  Term prev_term = prev == 0 ? 0 : log_[prev - 1].term;
  AppendEntries ae{term_, id_, prev, prev_term, {}, commit_index_};
  Index end = std::min<Index>(last_log_index(), prev + cfg_.max_entries_per_append);
  for (Index i = next; i <= end; ++i) ae.entries.push_back(log_[i - 1]);
  net_.send(id_, peer, std::move(ae));
}

void Node::broadcast_append() {
  for (NodeId p : peers_) replicate_to(p);
}

void Node::advance_commit_index() {
  if (role_ != Role::Leader) return;
  for (Index n = last_log_index(); n > commit_index_; --n) {
    if (log_[n - 1].term != term_) break;  // only current-term entries count
    int count = 1;  // ourselves
    for (NodeId p : peers_)
      if (match_index_[p] >= n) ++count;
    if (count * 2 > cluster_size()) {
      commit_index_ = n;
      break;
    }
  }
  apply_committed();
}

void Node::apply_committed() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const LogEntry& e = log_[last_applied_ - 1];
    if (!e.command.empty() && on_apply) on_apply(last_applied_, e);  // skip no-ops
  }
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
  } else if(const auto* aer = std::get_if<AppendEntriesReply>(&env.msg)) {
    handle_append_entries_reply(env.from, *aer);
  }
}

}  // namespace raft