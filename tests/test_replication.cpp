#include <memory>
#include <string>
#include <vector>

#include "check.h"
#include "cluster.h"

using namespace raft;
using Cmds = std::vector<std::string>;

namespace {

// ---------------------------------------------------------------------------
// Follower logic, one AppendEntries at a time. Node 0 of 3 starts with a given
// term and log; node 1 plays the leader and receives the replies.
// ---------------------------------------------------------------------------

struct FollowerHarness {
  Simulator sim{9};
  Network net{sim, 3};
  MemoryStorage storage;
  std::unique_ptr<Node> node;
  std::vector<AppendEntriesReply> replies;
  Cmds applied;

  FollowerHarness(Term term, std::vector<LogEntry> log) {
    storage.save({term, -1, std::move(log)});
    node = std::make_unique<Node>(0, std::vector<NodeId>{1, 2}, sim, net, storage);
    net.attach(1, [this](const Envelope& e) {
      if (const auto* r = std::get_if<AppendEntriesReply>(&e.msg)) replies.push_back(*r);
    });
    node->on_apply = [this](Index, const LogEntry& e) { applied.push_back(e.command); };
    node->start();
  }

  AppendEntriesReply send(AppendEntries ae) {
    replies.clear();
    node->on_message({1, 0, std::move(ae)});
    sim.run_for(10 * kMs);  // deliver the reply, well before any election timeout
    return replies.empty() ? AppendEntriesReply{999, false, 0, 0} : replies.back();
  }
};

AppendEntries ae(Term term, Index prev, Term prev_term, std::vector<LogEntry> entries,
                 Index commit) {
  return AppendEntries{term, 1, prev, prev_term, std::move(entries), commit};
}

void test_follower_rejects_stale_term() {
  FollowerHarness h(5, {{1, "a"}});
  auto r = h.send(ae(3, 0, 0, {{3, "x"}}, 0));
  CHECK(!r.success);
  CHECK(r.term == 5);  // tells the old leader the real term
  CHECK(h.node->log().size() == 1);
}

void test_follower_rejects_missing_prev_entry() {
  FollowerHarness h(1, {{1, "a"}, {1, "b"}});
  auto r = h.send(ae(1, 5, 1, {}, 0));  // leader thinks we have 5 entries
  CHECK(!r.success);
  CHECK(r.conflict_index == 3);  // "my log ends at 2; start from 3"
  CHECK(h.node->log().size() == 2);
}

void test_follower_rejects_prev_term_mismatch_with_hint() {
  // Terms by index: 1:t1, 2:t2, 3:t2, 4:t2
  FollowerHarness h(2, {{1, "a"}, {2, "b"}, {2, "c"}, {2, "d"}});
  auto r = h.send(ae(3, 4, 3, {}, 0));  // leader says entry 4 is from term 3
  CHECK(!r.success);
  CHECK(r.term == 3);
  CHECK(r.conflict_index == 2);  // skip the whole term-2 run, back to its start
  CHECK(h.node->log().size() == 4);  // rejection must not modify the log
}

void test_follower_appends_and_persists() {
  FollowerHarness h(1, {{1, "a"}});
  auto r = h.send(ae(1, 1, 1, {{1, "b"}, {1, "c"}}, 0));
  CHECK(r.success);
  CHECK(r.match_index == 3);
  CHECK(h.node->log().size() == 3);
  CHECK(h.storage.load().log.size() == 3);  // durable before the reply
}

void test_follower_truncates_on_conflict() {
  FollowerHarness h(1, {{1, "a"}, {1, "b"}, {1, "c"}});
  // A new leader (term 2) says index 2 is its own entry "X".
  auto r = h.send(ae(2, 1, 1, {{2, "X"}}, 0));
  CHECK(r.success);
  CHECK(r.match_index == 2);
  CHECK(h.node->log().size() == 2);  // old "b" and "c" are gone
  CHECK(h.node->log()[1].command == "X");
  CHECK(h.node->log()[1].term == 2);
  CHECK(h.storage.load().log.size() == 2);
}

void test_follower_stale_append_does_not_truncate() {
  // A delayed, duplicated message carrying only the first entry arrives after
  // we already have three. It matches, so nothing may be deleted.
  FollowerHarness h(1, {{1, "a"}, {1, "b"}, {1, "c"}});
  auto r = h.send(ae(1, 0, 0, {{1, "a"}}, 0));
  CHECK(r.success);
  CHECK(r.match_index == 1);
  CHECK(h.node->log().size() == 3);
  CHECK(h.node->log()[2].command == "c");
}

void test_follower_commit_capped_at_last_new_entry() {
  FollowerHarness h(1, {{1, "a"}, {1, "b"}, {1, "c"}});
  // Leader's commit index is 3, but this message only vouches for entry 1. We
  // must not commit b and c: they might be leftovers from an old leader.
  h.send(ae(1, 0, 0, {{1, "a"}}, 3));
  CHECK(h.node->commit_index() == 1);
  CHECK((h.applied == Cmds{"a"}));
  // Now the leader vouches for the rest.
  h.send(ae(1, 1, 1, {{1, "b"}, {1, "c"}}, 3));
  CHECK(h.node->commit_index() == 3);
  CHECK((h.applied == Cmds{"a", "b", "c"}));
}

void test_follower_commit_index_never_decreases() {
  FollowerHarness h(1, {{1, "a"}, {1, "b"}});
  h.send(ae(1, 0, 0, {{1, "a"}, {1, "b"}}, 2));
  CHECK(h.node->commit_index() == 2);
  h.send(ae(1, 0, 0, {{1, "a"}}, 1));  // older message arrives late
  CHECK(h.node->commit_index() == 2);
}

void test_apply_skips_noop_entries() {
  FollowerHarness h(1, {{1, ""}, {1, "x"}});  // "" is the leader's no-op
  h.send(ae(1, 2, 1, {}, 2));
  CHECK(h.node->commit_index() == 2);
  CHECK((h.applied == Cmds{"x"}));
  CHECK(h.node->last_applied() == 2);
}

// ---------------------------------------------------------------------------
// The leader's commit rule (Raft section 5.4.2, "Figure 8").
// ---------------------------------------------------------------------------

void test_leader_commits_only_current_term_entries_by_counting() {
  Simulator sim(3);
  Network net(sim, 3);
  MemoryStorage st;
  st.save({1, -1, {{1, "x"}}});  // one entry left over from term 1
  Node n(0, {1, 2}, sim, net, st);
  n.start();
  sim.run_for(400 * kMs);  // election timeout: becomes a candidate
  n.on_message({1, 0, RequestVoteReply{n.term(), true}});  // majority of 3
  CHECK(n.role() == Role::Leader);
  CHECK(n.log().size() == 2);  // "x" plus the new term's no-op
  Term t = n.term();

  // Follower 1 holds only the old-term entry. That is a majority for index 1,
  // but committing it by counting would be unsafe, so commit must not move.
  n.on_message({1, 0, AppendEntriesReply{t, true, 1, 0}});
  CHECK(n.commit_index() == 0);

  // Once a majority holds an entry from the current term, everything before
  // it commits too.
  n.on_message({1, 0, AppendEntriesReply{t, true, 2, 0}});
  CHECK(n.commit_index() == 2);
}

void test_leader_ignores_stale_and_duplicate_replies() {
  Simulator sim(3);
  Network net(sim, 3);
  MemoryStorage st;
  Node n(0, {1, 2}, sim, net, st);
  n.start();
  sim.run_for(400 * kMs);
  n.on_message({1, 0, RequestVoteReply{n.term(), true}});
  CHECK(n.role() == Role::Leader);
  Term t = n.term();

  n.on_message({1, 0, AppendEntriesReply{t, true, 1, 0}});
  CHECK(n.commit_index() == 1);
  n.on_message({1, 0, AppendEntriesReply{t, true, 0, 0}});  // late, older reply
  CHECK(n.commit_index() == 1);                              // did not regress
  n.on_message({1, 0, AppendEntriesReply{t - 1, true, 1, 0}});  // wrong term
  CHECK(n.role() == Role::Leader);
}

// ---------------------------------------------------------------------------
// Whole-cluster scenarios
// ---------------------------------------------------------------------------

void test_single_node_commits_alone() {
  Cluster c(1, 1);
  c.run(1 * kSec);
  CHECK(c.leader() == 0);
  CHECK(c.nodes[0]->propose("a") > 0);
  CHECK(c.nodes[0]->propose("b") > 0);
  CHECK((c.applied_commands(0) == Cmds{"a", "b"}));  // no network needed
}

void test_propose_rejected_unless_leader() {
  Cluster c(2, 3);
  c.run(2 * kSec);
  NodeId l = c.leader();
  CHECK(l >= 0);
  if (l < 0) return;
  NodeId follower = (l + 1) % 3;
  CHECK(c.nodes[follower]->propose("nope") == 0);
  CHECK(c.nodes[l]->propose("") == 0);  // empty is reserved for the no-op
  CHECK(c.nodes[l]->propose("yes") > 0);
}

void test_replicates_to_all_nodes() {
  Cluster c(3, 5);
  c.run(2 * kSec);
  Cmds sent;
  for (int i = 0; i < 20; ++i) {
    sent.push_back("cmd" + std::to_string(i));
    CHECK(c.propose(sent.back()) > 0);
  }
  c.run(1 * kSec);
  for (int i = 0; i < 5; ++i) CHECK(c.applied_commands(i) == sent);
  CHECK(c.logs_identical());
  c.check_log_matching();
  CHECK(c.violations.empty());
}

void test_large_backlog_streams_in_batches() {
  // 300 entries is far more than one AppendEntries batch (64).
  Cluster c(4, 3);
  c.run(2 * kSec);
  NodeId l = c.leader();
  CHECK(l >= 0);
  if (l < 0) return;
  int down = (l + 1) % 3;
  c.crash(down);
  Cmds sent;
  for (int i = 0; i < 300; ++i) {
    sent.push_back("e" + std::to_string(i));
    c.nodes[l]->propose(sent.back());
  }
  c.run(1 * kSec);
  c.restart(down);
  c.run(3 * kSec);
  CHECK(c.applied_commands(down) == sent);  // replayed everything after restart
  CHECK(c.logs_identical());
  CHECK(c.violations.empty());
}

void test_follower_catches_up_after_crash() {
  Cluster c(5, 5);
  c.run(2 * kSec);
  NodeId l = c.leader();
  CHECK(l >= 0);
  if (l < 0) return;
  int down = (l + 1) % 5;
  c.crash(down);
  Cmds sent;
  for (int i = 0; i < 10; ++i) {
    sent.push_back("v" + std::to_string(i));
    c.propose(sent.back());
  }
  c.run(1 * kSec);
  CHECK(c.applied_commands(l) == sent);  // majority of 4 still commits
  c.restart(down);
  c.run(2 * kSec);
  CHECK(c.applied_commands(down) == sent);
  CHECK(c.logs_identical());
}

void test_no_commit_without_majority() {
  Cluster c(6, 5);
  c.run(2 * kSec);
  NodeId l = c.leader();
  CHECK(l >= 0);
  if (l < 0) return;
  // Leave the leader with one follower: 2 of 5, not a majority.
  NodeId buddy = (l + 1) % 5;
  std::vector<NodeId> others;
  for (int i = 0; i < 5; ++i)
    if (i != l && i != buddy) others.push_back(i);
  c.net.partition({{l, buddy}, others});
  c.nodes[l]->propose("lonely");
  c.run(1 * kSec);
  for (int i = 0; i < 5; ++i)
    for (const auto& cmd : c.applied_commands(i)) CHECK(cmd != "lonely");
  CHECK(c.violations.empty());
}

void test_uncommitted_entries_overwritten_after_partition_heals() {
  Cluster c(7, 5);
  c.run(2 * kSec);
  NodeId old_leader = c.leader();
  CHECK(old_leader >= 0);
  if (old_leader < 0) return;
  NodeId buddy = (old_leader + 1) % 5;
  std::vector<NodeId> majority;
  for (int i = 0; i < 5; ++i)
    if (i != old_leader && i != buddy) majority.push_back(i);

  c.net.partition({{old_leader, buddy}, majority});
  // The stranded leader keeps accepting commands it can never commit.
  c.nodes[old_leader]->propose("lost1");
  c.nodes[old_leader]->propose("lost2");
  c.run(2 * kSec);

  // The majority side elects its own leader and commits real work.
  NodeId new_leader = -1;
  for (NodeId l : c.leaders())
    if (l != old_leader) new_leader = l;
  CHECK(new_leader >= 0);
  if (new_leader < 0) return;
  c.nodes[new_leader]->propose("kept1");
  c.nodes[new_leader]->propose("kept2");
  c.run(1 * kSec);

  c.net.heal();
  c.run(3 * kSec);

  // Everyone converges on the majority's history; the stranded entries vanish.
  for (int i = 0; i < 5; ++i) CHECK((c.applied_commands(i) == Cmds{"kept1", "kept2"}));
  CHECK(c.logs_identical());
  CHECK(c.leaders().size() == 1);
  c.check_log_matching();
  CHECK(c.violations.empty());
}

void test_committed_entries_survive_leader_crash() {
  Cluster c(8, 5);
  c.run(2 * kSec);
  Cmds first, second;
  for (int i = 0; i < 5; ++i) {
    first.push_back("a" + std::to_string(i));
    c.propose(first.back());
  }
  c.run(1 * kSec);
  NodeId dead = c.leader();
  c.crash(dead);
  c.run(2 * kSec);
  CHECK(c.leader() >= 0);
  for (int i = 0; i < 5; ++i) {
    second.push_back("b" + std::to_string(i));
    c.propose(second.back());
  }
  c.run(1 * kSec);
  c.restart(dead);
  c.run(2 * kSec);

  Cmds all = first;
  all.insert(all.end(), second.begin(), second.end());
  for (int i = 0; i < 5; ++i) CHECK(c.applied_commands(i) == all);
  CHECK(c.violations.empty());
}

// ---------------------------------------------------------------------------
// Chaos: random crashes, partitions, loss, duplication, reordering, with
// clients proposing the whole time (including to stranded, stale leaders).
// ---------------------------------------------------------------------------

void test_chaos_replication() {
  int bad_seeds = 0;
  for (std::uint64_t seed = 1; seed <= 30; ++seed) {
    Cluster c(seed, 5);
    c.net.set_delay(1 * kMs, 20 * kMs);
    c.net.set_drop_rate(0.15);
    c.net.set_duplicate_rate(0.10);

    std::set<std::string> proposed;
    int next_cmd = 0;
    auto& rng = c.sim.rng();

    for (int round = 0; round < 80; ++round) {
      // Every node that believes it leads gets a command, stale ones included.
      for (NodeId l : c.leaders()) {
        std::string cmd = "c" + std::to_string(next_cmd++);
        if (c.nodes[l]->propose(cmd) > 0) proposed.insert(cmd);
      }
      c.run(100 * kMs);

      int victim = static_cast<int>(rng() % 5);
      switch (rng() % 4) {
        case 0:
          if (c.nodes[victim]->running()) c.crash(victim);
          break;
        case 1:
          if (!c.nodes[victim]->running()) c.restart(victim);
          break;
        case 2: {
          std::vector<NodeId> a, b;
          for (int i = 0; i < 5; ++i) (rng() % 2 ? a : b).push_back(i);
          c.net.partition({a, b});
          break;
        }
        case 3:
          c.net.heal();
          break;
      }
      if (round % 10 == 0) c.check_log_matching();
    }

    // Stop the chaos and let the cluster settle.
    c.net.heal();
    c.net.set_drop_rate(0.0);
    c.net.set_duplicate_rate(0.0);
    for (int i = 0; i < 5; ++i)
      if (!c.nodes[i]->running()) c.restart(i);
    c.run(5 * kSec);

    bool ok = c.violations.empty() && c.election_safe();
    NodeId l = c.leader();
    if (l < 0) {
      ok = false;
    } else {
      // Liveness: one more command must commit on every node.
      c.nodes[l]->propose("final");
      proposed.insert("final");
      c.run(2 * kSec);
      c.check_log_matching();
      ok = ok && c.violations.empty() && c.logs_identical() && c.applied_identical();

      Cmds applied = c.applied_commands(0);
      ok = ok && !applied.empty() && applied.back() == "final";
      std::set<std::string> seen;
      for (const auto& cmd : applied) {
        ok = ok && proposed.count(cmd) == 1;  // nothing invented
        ok = ok && seen.insert(cmd).second;   // nothing applied twice
      }
    }
    if (!ok) {
      ++bad_seeds;
      std::fprintf(stderr, "  chaos seed %llu failed\n", (unsigned long long)seed);
      c.print_violations();
    }
  }
  CHECK(bad_seeds == 0);
}

}  // namespace

int main() {
  const TestCase tests[] = {
      {"follower: rejects stale term", test_follower_rejects_stale_term},
      {"follower: rejects missing prev entry", test_follower_rejects_missing_prev_entry},
      {"follower: rejects prev term mismatch, hints", test_follower_rejects_prev_term_mismatch_with_hint},
      {"follower: appends and persists", test_follower_appends_and_persists},
      {"follower: truncates on conflict", test_follower_truncates_on_conflict},
      {"follower: stale append does not truncate", test_follower_stale_append_does_not_truncate},
      {"follower: commit capped at last new entry", test_follower_commit_capped_at_last_new_entry},
      {"follower: commit index never decreases", test_follower_commit_index_never_decreases},
      {"follower: apply skips no-ops", test_apply_skips_noop_entries},
      {"leader: commits only current-term entries by counting",
       test_leader_commits_only_current_term_entries_by_counting},
      {"leader: ignores stale and duplicate replies", test_leader_ignores_stale_and_duplicate_replies},
      {"cluster: single node commits alone", test_single_node_commits_alone},
      {"cluster: propose rejected unless leader", test_propose_rejected_unless_leader},
      {"cluster: replicates to all nodes", test_replicates_to_all_nodes},
      {"cluster: large backlog streams in batches", test_large_backlog_streams_in_batches},
      {"cluster: follower catches up after crash", test_follower_catches_up_after_crash},
      {"cluster: no commit without majority", test_no_commit_without_majority},
      {"cluster: uncommitted entries overwritten after heal",
       test_uncommitted_entries_overwritten_after_partition_heals},
      {"cluster: committed entries survive leader crash", test_committed_entries_survive_leader_crash},
      {"chaos: replication safety and liveness (30 seeds)", test_chaos_replication},
  };
  return run_tests(tests, sizeof(tests) / sizeof(tests[0]));
}