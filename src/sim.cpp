#include "raft/sim.h"

namespace raft {

Simulator::TimerId Simulator::schedule(Time delay, Callback cb) {
  TimerId id = next_id_++;
  Time at = now_ + delay;
  events_.emplace(Key{at, id}, std::move(cb));
  fire_time_.emplace(id, at);
  return id;
}

void Simulator::cancel(TimerId id) {
  auto it = fire_time_.find(id);
  if (it == fire_time_.end()) return;
  events_.erase(Key{it->second, id});
  fire_time_.erase(it);
}

bool Simulator::step() {
  if (events_.empty()) return false;
  auto it = events_.begin();
  Key key = it->first;
  Callback cb = std::move(it->second);
  events_.erase(it);
  fire_time_.erase(key.second);
  now_ = key.first;
  cb();  // may schedule or cancel other events
  return true;
}

void Simulator::run_until(Time t) {
  while (!events_.empty() && events_.begin()->first.first <= t) step();
  if (now_ < t) now_ = t;
}

}  // namespace raft
