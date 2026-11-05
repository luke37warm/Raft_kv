#pragma once
// Persistent state: the three things Raft must survive a crash with
// (currentTerm, votedFor, log). Everything else is rebuilt after restart.
//
// For now `save` rewrites the whole state. Milestone 5 swaps in a file-backed
// implementation with fsync and incremental log appends behind this interface.

#include <vector>

#include "raft/types.h"

namespace raft {

struct PersistentState {
  Term current_term = 0;
  NodeId voted_for = -1;  // -1 => none this term
  std::vector<LogEntry> log;
};

class Storage {
 public:
  virtual ~Storage() = default;
  virtual PersistentState load() = 0;
  virtual void save(const PersistentState& state) = 0;
};

// Survives Node::crash() because it lives outside the node, which is exactly
// the property a disk gives us.
class MemoryStorage : public Storage {
 public:
  PersistentState load() override { return state_; }
  void save(const PersistentState& state) override {
    state_ = state;
    ++save_count_;
  }
  std::size_t save_count() const { return save_count_; }

 private:
  PersistentState state_;
  std::size_t save_count_ = 0;
};

}  // namespace raft
