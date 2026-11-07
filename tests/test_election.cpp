#include <map>
#include <memory>
#include <set>
#include <vector>

#include "check.h"
#include "raft/network.h"
#include "raft/node.h"
#include "raft/sim.h"
#include "raft/storage.h"

using namespace raft;

namespace {

constexpr Time kMs = 1000;
constexpr Time kSec = 1000 * kMs;

// A whole cluster of nodes wired through the simulated network. It records
// every leadership win so tests can check Raft's core safety property:
// at most one leader per term.
struct Cluster {
  Simulator sim;
  Network net;
  int n;
  std::vector<std::unique_ptr<MemoryStorage>> storage;
  std::vector<std::unique_ptr<Node>> nodes;
  std::map<Term, std::set<NodeId>> leaders_by_term;

  Cluster(std::uint64_t seed, int num) : sim(seed), net(sim, num), n(num) {
    for (int i = 0; i < n; ++i) {
      std::vector<NodeId> peers;
      for (int j = 0; j < n; ++j)
        if (j != i) peers.push_back(j);
      storage.push_back(std::make_unique<MemoryStorage>());
      nodes.push_back(std::make_unique<Node>(i, peers, sim, net, *storage[i]));
      nodes[i]->on_become_leader = [this](NodeId id, Term t) {
        leaders_by_term[t].insert(id);
      };
      net.attach(i, [this, i](const Envelope& e) { nodes[i]->on_message(e); });
    }
    for (auto& nd : nodes) nd->start();
  }

  void crash(int i) {
    nodes[i]->crash();
    net.set_node_up(i, false);
  }
  void restart(int i) {
    net.set_node_up(i, true);
    nodes[i]->restart();
  }

  // Running nodes that currently believe they are leader. During a partition
  // this can legitimately contain a stale leader from an older term.
  std::vector<NodeId> leaders() const {
    std::vector<NodeId> out;
    for (int i = 0; i < n; ++i)
      if (nodes[i]->running() && nodes[i]->role() == Role::Leader) out.push_back(i);
    return out;
  }

  bool election_safe() const {
    for (const auto& [term, ids] : leaders_by_term)
      if (ids.size() > 1) return false;
    return true;
  }
};

void test_single_node_cluster() {
  Cluster c(1, 1);
  c.sim.run_for(1 * kSec);
  CHECK(c.leaders().size() == 1);
  CHECK(c.nodes[0]->term() >= 1);
}

void test_elects_exactly_one_leader() {
  Cluster c(1, 5);
  c.sim.run_for(2 * kSec);
  auto ls = c.leaders();
  CHECK(ls.size() == 1);
  if (ls.size() != 1) return;
  Term t = c.nodes[ls[0]]->term();
  for (int i = 0; i < 5; ++i) {
    CHECK(c.nodes[i]->term() == t);  // everyone converged on the leader's term
    CHECK(c.nodes[i]->leader_id() == ls[0]);
    if (i != ls[0]) CHECK(c.nodes[i]->role() == Role::Follower);
  }
  CHECK(c.election_safe());
}

void test_leader_is_stable() {
  Cluster c(2, 5);
  c.sim.run_for(2 * kSec);
  auto ls = c.leaders();
  CHECK(ls.size() == 1);
  if (ls.size() != 1) return;
  Term t = c.nodes[ls[0]]->term();
  c.sim.run_for(30 * kSec);  // heartbeats must keep everyone quiet
  CHECK(c.leaders() == ls);
  CHECK(c.nodes[ls[0]]->term() == t);  // no spurious elections
}

void test_leader_crash_triggers_reelection() {
  Cluster c(3, 5);
  c.sim.run_for(2 * kSec);
  auto ls = c.leaders();
  CHECK(ls.size() == 1);
  if (ls.size() != 1) return;
  NodeId old_leader = ls[0];
  Term old_term = c.nodes[old_leader]->term();

  c.crash(old_leader);
  c.sim.run_for(2 * kSec);
  auto now = c.leaders();
  CHECK(now.size() == 1);
  if (now.size() != 1) return;
  CHECK(now[0] != old_leader);
  CHECK(c.nodes[now[0]]->term() > old_term);

  // The old leader comes back, sees a higher term in the first heartbeat,
  // and falls in line as a follower.
  c.restart(old_leader);
  c.sim.run_for(1 * kSec);
  CHECK(c.leaders() == now);
  CHECK(c.nodes[old_leader]->role() == Role::Follower);
  CHECK(c.nodes[old_leader]->term() == c.nodes[now[0]]->term());
  CHECK(c.election_safe());
}

void test_partitioned_leader_steps_down_after_heal() {
  Cluster c(4, 5);
  c.sim.run_for(2 * kSec);
  auto ls = c.leaders();
  CHECK(ls.size() == 1);
  if (ls.size() != 1) return;
  NodeId old_leader = ls[0];
  Term old_term = c.nodes[old_leader]->term();

  // Cut the leader off alone. The 4-node majority elects a new leader; the
  // isolated one keeps believing it leads (it cannot hear otherwise).
  std::vector<NodeId> majority;
  for (int i = 0; i < 5; ++i)
    if (i != old_leader) majority.push_back(i);
  c.net.partition({{old_leader}, majority});
  c.sim.run_for(2 * kSec);

  CHECK(c.leaders().size() == 2);  // stale leader + the real one
  CHECK(c.nodes[old_leader]->role() == Role::Leader);
  CHECK(c.nodes[old_leader]->term() == old_term);
  CHECK(c.election_safe());  // different terms, so still safe

  c.net.heal();
  c.sim.run_for(1 * kSec);
  auto after = c.leaders();
  CHECK(after.size() == 1);
  CHECK(c.nodes[old_leader]->role() == Role::Follower);
  CHECK(c.nodes[old_leader]->term() > old_term);
  CHECK(c.election_safe());
}

void test_no_leader_without_majority() {
  Cluster c(5, 5);
  c.sim.run_for(2 * kSec);
  c.crash(0);
  c.crash(1);
  c.crash(2);
  c.sim.run_for(10 * kSec);
  // Two survivors can never reach 3 votes, no matter how often they retry.
  CHECK(c.leaders().empty());
  CHECK(c.election_safe());
}

// --- Vote-granting rules, one RPC at a time ---------------------------------

struct VoteHarness {
  Simulator sim{9};
  Network net{sim, 3};
  MemoryStorage storage;
  std::unique_ptr<Node> node;
  std::vector<RequestVoteReply> replies;  // what node 1 receives

  // The node under test (id 0) starts with term 2 and one log entry from term 2.
  VoteHarness() {
    storage.save({2, -1, {{2, "a"}}});
    node = std::make_unique<Node>(0, std::vector<NodeId>{1, 2}, sim, net, storage);
    // Replies go back to whichever candidate asked, so capture both inboxes.
    for (NodeId id : {1, 2}) {
      net.attach(id, [this](const Envelope& e) {
        if (const auto* r = std::get_if<RequestVoteReply>(&e.msg)) replies.push_back(*r);
      });
    }
    node->start();
  }

  RequestVoteReply ask(NodeId cand, Term term, Index last_idx, Term last_term) {
    replies.clear();
    node->on_message({cand, 0, RequestVote{term, cand, last_idx, last_term}});
    sim.run_for(10 * kMs);  // deliver the reply (well before any election timeout)
    return replies.empty() ? RequestVoteReply{999, false} : replies.back();
  }
};

void test_vote_denied_for_stale_term() {
  VoteHarness h;
  auto r = h.ask(1, 1, 5, 5);  // term 1 < our term 2
  CHECK(!r.granted);
  CHECK(r.term == 2);  // reply tells the candidate the real term
}

void test_vote_denied_for_older_log_term() {
  VoteHarness h;
  auto r = h.ask(1, 3, 99, 1);  // long log, but last entry from older term
  CHECK(!r.granted);
  CHECK(h.node->term() == 3);  // we still adopted the newer term
}

void test_vote_denied_for_shorter_log() {
  VoteHarness h;
  auto r = h.ask(1, 3, 0, 2);  // same last term, but shorter log than our 1 entry
  CHECK(!r.granted);
}

void test_vote_granted_and_persisted() {
  VoteHarness h;
  auto r = h.ask(1, 3, 1, 2);
  CHECK(r.granted);
  CHECK(r.term == 3);
  CHECK(h.node->voted_for() == 1);
  CHECK(h.storage.load().voted_for == 1);  // on "disk" before the reply left
  CHECK(h.storage.load().current_term == 3);
}

void test_one_vote_per_term() {
  VoteHarness h;
  CHECK(h.ask(1, 3, 1, 2).granted);
  CHECK(!h.ask(2, 3, 1, 2).granted);  // different candidate, same term: no
  CHECK(h.ask(1, 3, 1, 2).granted);   // retry from the same candidate: yes
  CHECK(h.ask(2, 4, 1, 2).granted);   // new term, new vote
}

void test_vote_survives_crash() {
  VoteHarness h;
  CHECK(h.ask(1, 3, 1, 2).granted);
  h.node->crash();
  h.node->restart();
  CHECK(h.node->voted_for() == 1);
  CHECK(!h.ask(2, 3, 1, 2).granted);  // must not vote twice in term 3
}

// Regression test for a bug the chaos test found: the network can duplicate a
// vote reply, and counting replies instead of distinct voters let a candidate
// win with a minority.
void test_duplicate_vote_replies_do_not_count_twice() {
  Simulator sim(1);
  Network net(sim, 5);
  MemoryStorage st;
  Node n(0, {1, 2, 3, 4}, sim, net, st);
  int wins = 0;
  n.on_become_leader = [&](NodeId, Term) { ++wins; };
  n.start();
  sim.run_for(400 * kMs);  // times out and becomes a candidate
  CHECK(n.role() == Role::Candidate);
  RequestVoteReply grant{n.term(), true};
  n.on_message({1, 0, grant});
  n.on_message({1, 0, grant});  // same voter again
  n.on_message({1, 0, grant});
  CHECK(n.role() == Role::Candidate);  // 2 distinct votes of 5: not a majority
  CHECK(wins == 0);
  n.on_message({2, 0, grant});  // third distinct voter
  CHECK(n.role() == Role::Leader);
  CHECK(wins == 1);
}

// --- Randomized chaos ---------------------------------------------------------

void test_chaos_election_safety_and_liveness() {
  int unsafe_seeds = 0, stuck_seeds = 0;
  for (std::uint64_t seed = 1; seed <= 40; ++seed) {
    Cluster c(seed, 5);
    c.net.set_delay(1 * kMs, 20 * kMs);
    c.net.set_drop_rate(0.2);
    c.net.set_duplicate_rate(0.1);

    auto& rng = c.sim.rng();
    for (int round = 0; round < 60; ++round) {
      c.sim.run_for(100 * kMs);
      int victim = static_cast<int>(rng() % 5);
      switch (rng() % 4) {
        case 0:
          if (c.nodes[victim]->running()) c.crash(victim);
          break;
        case 1:
          if (!c.nodes[victim]->running()) c.restart(victim);
          break;
        case 2: {  // random split into two groups
          std::vector<NodeId> a, b;
          for (int i = 0; i < 5; ++i) (rng() % 2 ? a : b).push_back(i);
          c.net.partition({a, b});
          break;
        }
        case 3:
          c.net.heal();
          break;
      }
    }

    if (!c.election_safe()) ++unsafe_seeds;

    // Stop the chaos: heal, make the network reliable, revive everyone.
    c.net.heal();
    c.net.set_drop_rate(0.0);
    c.net.set_duplicate_rate(0.0);
    for (int i = 0; i < 5; ++i)
      if (!c.nodes[i]->running()) c.restart(i);
    c.sim.run_for(5 * kSec);

    auto ls = c.leaders();
    if (ls.size() != 1) {
      ++stuck_seeds;
      continue;
    }
    for (int i = 0; i < 5; ++i)
      if (c.nodes[i]->term() != c.nodes[ls[0]]->term()) ++stuck_seeds;
    if (!c.election_safe()) ++unsafe_seeds;
  }
  CHECK(unsafe_seeds == 0);  // safety: never two leaders in one term
  CHECK(stuck_seeds == 0);   // liveness: a leader emerges once the network heals
}

}  // namespace

int main() {
  const TestCase tests[] = {
      {"election: single-node cluster", test_single_node_cluster},
      {"election: exactly one leader", test_elects_exactly_one_leader},
      {"election: leader is stable", test_leader_is_stable},
      {"election: leader crash triggers re-election", test_leader_crash_triggers_reelection},
      {"election: partitioned leader steps down after heal",
       test_partitioned_leader_steps_down_after_heal},
      {"election: no leader without majority", test_no_leader_without_majority},
      {"vote: denied for stale term", test_vote_denied_for_stale_term},
      {"vote: denied for older log term", test_vote_denied_for_older_log_term},
      {"vote: denied for shorter log", test_vote_denied_for_shorter_log},
      {"vote: granted and persisted", test_vote_granted_and_persisted},
      {"vote: one per term", test_one_vote_per_term},
      {"vote: survives crash", test_vote_survives_crash},
      {"vote: duplicate replies counted once", test_duplicate_vote_replies_do_not_count_twice},
      {"chaos: election safety and liveness (40 seeds)",
       test_chaos_election_safety_and_liveness},
  };
  return run_tests(tests, sizeof(tests) / sizeof(tests[0]));
}