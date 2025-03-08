//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <deque>
#include <mutex>  // NOLINT
#include <optional>
#include <map>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  explicit LRUKNode(size_t k, frame_id_t frame_id);

  void set_evictable(bool set_evictable) { is_evictable_ = set_evictable; }
  void access(size_t timestamp, AccessType access_type);
  size_t distance() const;

  size_t fid() const { return fid_;  }
  bool evictable() const { return is_evictable_; };
  size_t last_access() const { return history_.empty() ? inf : history_.back(); }

  void clear();
  bool operator == (frame_id_t fid) const { return fid_ == fid; }
  bool operator < (const LRUKNode& other) const;

 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
  std::deque<size_t> history_{};
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};

  static constexpr size_t inf{SIZE_MAX};
};

struct LRUKNodePtrComparator {
    bool operator()(const std::shared_ptr<LRUKNode>& lhs, const std::shared_ptr<LRUKNode>& rhs) const {
        return *lhs < *rhs;
    }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  void check_frame_id_(frame_id_t frame_id) const;

  void tick_();

  std::map<std::shared_ptr<LRUKNode>, frame_id_t, LRUKNodePtrComparator> node_store_;
  std::vector<std::shared_ptr<LRUKNode>> nodes_;

  size_t current_timestamp_{0};
  size_t k_;
  size_t replacer_size_;
  std::mutex latch_{};

  size_t evictable_frames_{};
};

}  // namespace bustub
