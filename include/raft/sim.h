#pragma once
// Deterministic discrete-event simulator with a virtual clock.
//
// Nothing here uses wall-clock time or threads: time only advances when we
// pop the next event. A run is fully determined by the seed, so any failing
// chaos test can be replayed exactly.

#include <cstdint>
#include <functional>
#include <map>
#include <random>
#include <unordered_map>
#include <utility>

#include "raft/types.h"

namespace raft {

class Simulator {
 public:
  using Callback = std::function<void()>;
  using TimerId = std::uint64_t;

  explicit Simulator(std::uint64_t seed) : rng_(seed) {}

  Time now() const { return now_; }
  std::mt19937_64& rng() { return rng_; }

  // Run `cb` after `delay` virtual microseconds. Events at the same instant
  // run in the order they were scheduled.
  TimerId schedule(Time delay, Callback cb);

  // Cancel a pending event. Cancelling an already-fired id is a no-op.
  void cancel(TimerId id);

  // Run the single next event. Returns false if the queue is empty.
  bool step();

  // Run all events with time <= t, then set the clock to t.
  void run_until(Time t);
  void run_for(Time duration) { run_until(now_ + duration); }

  std::size_t pending() const { return events_.size(); }

 private:
  using Key = std::pair<Time, TimerId>;  // (fire time, sequence) => total order
  Time now_ = 0;
  TimerId next_id_ = 1;
  std::map<Key, Callback> events_;
  std::unordered_map<TimerId, Time> fire_time_;  // for O(log n) cancel
  std::mt19937_64 rng_;
};

}  // namespace raft
