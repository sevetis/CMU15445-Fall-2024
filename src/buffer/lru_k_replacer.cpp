//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"


namespace bustub {
    
LRUKNode::LRUKNode(size_t k, frame_id_t frame_id) : k_(k), fid_(frame_id) {}

void LRUKNode::access(size_t timestamp, [[maybe_unused]] AccessType access_type) {
    if (history_.size() == k_)
        history_.pop_front();
    history_.push_back(timestamp);
}

size_t LRUKNode::distance() const {
    if (history_.size() < k_)
    return inf;
return history_.back() - history_.front();
}

void LRUKNode::clear() {
    history_.clear();
}

bool LRUKNode::operator < (const LRUKNode& other) const {
    if (distance() > other.distance()) {
        return true;
    } else if (distance() == other.distance()) {
        return last_access() > other.last_access();
    }
    return false;
}


/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) 
    : nodes_(num_frames + 1, nullptr), k_(k), replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
    std::lock_guard<std::mutex> lock(latch_);
    if (not evictable_frames_)
        return std::nullopt;
    std::cout << "_++++++++++++++++++++++++++++++++\n mp size =" << node_store_.size() << std::endl;
    for (auto [k, v]: node_store_) {
        if (k != nullptr) {
            std::cout << "frame id = " << v << " last access time = " << k->last_access() << " distance = " << k->distance() << std::endl;
        } else {
            std::cout << "nullptr" << std::endl;
        }
    }

    const auto node = node_store_.begin()->first;
    node_store_.erase(node);
    const auto fid = node->fid();
    nodes_.at(fid).reset();
    return fid;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This paramle) : This method controls whether a frame is evictable or not. It also controls the LRUKReplacer's size. You'll know when to call this function when you implement the BufferPoolManager. To be specific, when the pin count of a page hits 0, its corresponding frame should be marked as evictable.eter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
    std::lock_guard<std::mutex> lock(latch_);
    tick_();
    check_frame_id_(frame_id);
    auto node = nodes_.at(frame_id) != nullptr ?
                nodes_.at(frame_id) : std::make_shared<LRUKNode>(LRUKNode(k_, frame_id));

    std::cout << "record access id = " << frame_id << " timestamp = " << current_timestamp_ << std::endl;
    node->access(current_timestamp_, access_type);
    if (nodes_.at(frame_id) == nullptr) {
        std::cout << "is nullptr" << std::endl;
        nodes_[frame_id] = node;
        node_store_[node] = frame_id;
        std::cout << "add size. size = " << node_store_.size() << std::endl;
    } else {
        std::cout << "is not nullptr" << std::endl;
        std::cout << "id = " << node->fid() << " distance = " << node->distance() << std::endl;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lock(latch_);
    check_frame_id_(frame_id);
    if (nodes_.at(frame_id) != nullptr && nodes_.at(frame_id)->evictable() != set_evictable) {
        nodes_.at(frame_id)->set_evictable(set_evictable);
        evictable_frames_ += set_evictable ? 1 : -1;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    check_frame_id_(frame_id);
    if (nodes_.at(frame_id).use_count() > 0) {
        BUSTUB_ASSERT(nodes_.at(frame_id)->evictable(), "an unevictable frame can not be remove");
        evictable_frames_ -= 1;
        nodes_.at(frame_id).reset();
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return evictable_frames_; }

void LRUKReplacer::check_frame_id_(frame_id_t frame_id) const {
    BUSTUB_ASSERT(0 < frame_id, "frame id less than one");
    BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "frame id larger than replacer size");
}

void LRUKReplacer::tick_() {
    current_timestamp_++;
}

}  // namespace bustub
