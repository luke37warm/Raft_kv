#include <tuple>
#include <utility>
#include <vector>

#include "check.h"
#include "raft/network.h"
#include "raft/node.h"
#include "raft/sim.h"
#include "raft/storage.h"

using namespace raft;

namespace {

struct Delivery {
  Time at;
  Envelope env;
};

// n nodes whose inboxes simply record what arrives and when.
struct Harness {
  Simulator sim;
  Network net;
  std::vector<std::vector<Delivery>> inbox;

  Harness(std::uint64_t seed, int n) : sim(seed), net(sim, n), inbox(n) {
    for (int i = 0; i < n; ++i) {
      net.attach(i, [this, i](const Envelope& e) {
        inbox[i].push_back({sim.now(), e});
      });
    }
  }
};

Message ping() { return RequestVote{1, 0, 0, 0}; }

void test_simulator_ordering_and_cancel() {
  Simulator sim(1);
  std::vector<int> order;
  sim.schedule(30, [&] { order.push_back(3); });
  sim.schedule(10, [&] { order.push_back(1); });
  sim.schedule(10, [&] { order.push_back(2); });  // same instant: FIFO
  auto doomed = sim.schedule(20, [&] { order.push_back(99); });
  sim.cancel(doomed);
  sim.cancel(doomed);  // double-cancel is harmless
  sim.run_for(100);
  CHECK((order == std::vector<int>{1, 2, 3}));
  CHECK(sim.now() == 100);
  CHECK(sim.pending() == 0);
}

void test_simulator_events_can_schedule_events() {
  Simulator sim(1);
  std::vector<Time> fired;
  sim.schedule(5, [&] {
    fired.push_back(sim.now());
    sim.schedule(5, [&] { fired.push_back(sim.now()); });
  });
  sim.run_for(100);
  CHECK((fired == std::vector<Time>{5, 10}));
}

void test_network_delay_bounds() {
  Harness h(2, 2);
  h.net.set_delay(1000, 5000);
  for (int i = 0; i < 50; ++i) h.net.send(0, 1, ping());
  h.sim.run_for(10000);
  CHECK(h.inbox[1].size() == 50);
  bool in_bounds = true;
  for (const auto& d : h.inbox[1]) in_bounds &= (d.at >= 1000 && d.at <= 5000);
  CHECK(in_bounds);
  CHECK(h.net.stats().sent == 50);
  CHECK(h.net.stats().delivered == 50);
  CHECK(h.net.stats().dropped == 0);
}

void test_network_drop_all() {
  Harness h(3, 2);
  h.net.set_drop_rate(1.0);
  for (int i = 0; i < 100; ++i) h.net.send(0, 1, ping());
  h.sim.run_for(100000);
  CHECK(h.inbox[1].empty());
  CHECK(h.net.stats().dropped == 100);
}

void test_network_duplicates() {
  Harness h(4, 2);
  h.net.set_duplicate_rate(1.0);
  h.net.send(0, 1, ping());
  h.sim.run_for(100000);
  CHECK(h.inbox[1].size() == 2);
}

void test_network_partition_and_heal() {
  Harness h(5, 3);
  h.net.partition({{0, 1}, {2}});
  h.net.send(0, 1, ping());  // same side: ok
  h.net.send(0, 2, ping());  // across: blocked
  h.net.send(2, 0, ping());  // across: blocked
  h.sim.run_for(100000);
  CHECK(h.inbox[1].size() == 1);
  CHECK(h.inbox[2].empty());
  CHECK(h.inbox[0].empty());

  h.net.heal();
  h.net.send(0, 2, ping());
  h.sim.run_for(100000);
  CHECK(h.inbox[2].size() == 1);
}

void test_network_partition_drops_in_flight() {
  Harness h(6, 3);
  h.net.send(0, 2, ping());  // accepted onto the wire...
  h.net.partition({{0, 1}, {2}});  // ...then the link breaks before arrival
  h.sim.run_for(100000);
  CHECK(h.inbox[2].empty());
  CHECK(h.net.stats().dropped == 1);
}

void test_network_unlisted_nodes_isolated() {
  Harness h(7, 3);
  h.net.partition({{0, 1}});  // node 2 not listed
  h.net.send(0, 2, ping());
  h.sim.run_for(100000);
  CHECK(h.inbox[2].empty());
}

void test_network_node_down() {
  Harness h(8, 2);
  h.net.set_node_up(1, false);
  h.net.send(0, 1, ping());
  h.net.send(1, 0, ping());
  h.sim.run_for(100000);
  CHECK(h.inbox[0].empty());
  CHECK(h.inbox[1].empty());
  h.net.set_node_up(1, true);
  h.net.send(0, 1, ping());
  h.sim.run_for(100000);
  CHECK(h.inbox[1].size() == 1);
}

using Trace = std::vector<std::tuple<Time, int, int, std::size_t>>;

Trace run_chaos(std::uint64_t seed) {
  Harness h(seed, 5);
  h.net.set_delay(100, 9000);
  h.net.set_drop_rate(0.3);
  h.net.set_duplicate_rate(0.2);
  for (int i = 0; i < 200; ++i) {
    NodeId from = static_cast<NodeId>(h.sim.rng()() % 5);
    NodeId to = static_cast<NodeId>(h.sim.rng()() % 5);
    h.net.send(from, to, ping());
    h.sim.run_for(50);
  }
  h.sim.run_for(1000000);
  Trace trace;
  for (int n = 0; n < 5; ++n)
    for (const auto& d : h.inbox[n])
      trace.emplace_back(d.at, d.env.from, d.env.to, d.env.msg.index());
  return trace;
}

void test_determinism() {
  CHECK(run_chaos(7) == run_chaos(7));  // same seed => identical history
  CHECK(!(run_chaos(7) == run_chaos(8)));
  CHECK(!run_chaos(7).empty());
}

void test_storage_roundtrip() {
  MemoryStorage st;
  CHECK(st.load().current_term == 0);
  CHECK(st.load().voted_for == -1);
  PersistentState s;
  s.current_term = 4;
  s.voted_for = 2;
  s.log = {{1, "a"}, {4, "b"}};
  st.save(s);
  PersistentState r = st.load();
  CHECK(r.current_term == 4);
  CHECK(r.voted_for == 2);
  CHECK(r.log.size() == 2 && r.log[1].command == "b" && r.log[1].term == 4);
}

void test_node_loads_state_and_steps_down() {
  Simulator sim(9);
  Network net(sim, 3);
  MemoryStorage st;
  st.save({3, 2, {{1, "x"}}});
  Node n(0, {1, 2}, sim, net, st);

  n.start();
  CHECK(n.running());
  CHECK(n.role() == Role::Follower);
  CHECK(n.term() == 3);
  CHECK(n.voted_for() == 2);
  CHECK(n.last_log_index() == 1);

  // Stale or equal terms change nothing.
  n.on_message({1, 0, RequestVote{3, 1, 0, 0}});
  CHECK(n.term() == 3 && n.voted_for() == 2);

  // Higher term: adopt it, clear the vote, and persist before anything else.
  n.on_message({1, 0, RequestVote{7, 1, 0, 0}});
  CHECK(n.term() == 7);
  CHECK(n.voted_for() == -1);
  CHECK(st.load().current_term == 7);
  CHECK(st.load().voted_for == -1);
}

void test_node_crash_and_restart() {
  Simulator sim(10);
  Network net(sim, 3);
  MemoryStorage st;
  Node n(0, {1, 2}, sim, net, st);
  n.start();
  n.on_message({1, 0, AppendEntriesReply{9, false, 0}});
  CHECK(n.term() == 9);

  n.crash();
  CHECK(!n.running());
  n.on_message({1, 0, AppendEntriesReply{99, false, 0}});  // ignored while down
  CHECK(st.load().current_term == 9);

  n.restart();
  CHECK(n.running());
  CHECK(n.term() == 9);  // survived the crash via storage
  CHECK(n.role() == Role::Follower);
}

void test_node_election_timer() {
  Simulator sim(11);
  Network net(sim, 3);
  MemoryStorage st;
  Node n(0, {1, 2}, sim, net, st);
  n.start();

  CHECK(n.election_timeouts() == 0);
  sim.run_for(299999);  // timeout is drawn from [150ms, 300ms]
  CHECK(n.election_timeouts() >= 1);

  std::uint64_t before = n.election_timeouts();
  n.crash();
  sim.run_for(5000000);
  CHECK(n.election_timeouts() == before);  // a crashed node's timer is dead
  CHECK(sim.pending() == 0);
}

}  // namespace

int main() {
  const TestCase tests[] = {
      {"simulator: ordering and cancel", test_simulator_ordering_and_cancel},
      {"simulator: events schedule events", test_simulator_events_can_schedule_events},
      {"network: delay bounds", test_network_delay_bounds},
      {"network: drop all", test_network_drop_all},
      {"network: duplicates", test_network_duplicates},
      {"network: partition and heal", test_network_partition_and_heal},
      {"network: partition drops in-flight", test_network_partition_drops_in_flight},
      {"network: unlisted nodes isolated", test_network_unlisted_nodes_isolated},
      {"network: node down", test_network_node_down},
      {"determinism: same seed same trace", test_determinism},
      {"storage: roundtrip", test_storage_roundtrip},
      {"node: loads state, steps down on higher term", test_node_loads_state_and_steps_down},
      {"node: crash and restart", test_node_crash_and_restart},
      {"node: election timer", test_node_election_timer},
  };
  return run_tests(tests, sizeof(tests) / sizeof(tests[0]));
}
