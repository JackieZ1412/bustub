//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    latch_.lock();
    current_timestamp_++;
    if(frames_map.empty()) {
        latch_.unlock();
        return false;
    }
    auto frame_iter = evictable_frames.begin();
    for(auto it = evictable_frames.begin(); it != evictable_frames.end();it++){
        if(frame_iter->GetKDist(k_) == -1 && it->GetKDist(k_) == -1){
            if(frame_iter->GetTimeStamp()[0] > it->GetTimeStamp()[0]){
                frame_iter = it;
            }
        } else if(frame_iter->GetKDist(k_) != -1 && it->GetKDist(k_) == -1){
            frame_iter = it;
        } else if(frame_iter->GetKDist(k_) != -1 && it->GetKDist(k_) != -1 && frame_iter->GetKDist(k_) > it->GetKDist(k_)){
            frame_iter = it;
        }
    }
    *frame_id = frame_iter->GetFrameID();
    evictable_frames.erase(frame_iter);
    frames_map.erase(*frame_id);
    latch_.unlock();
    return true;

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_ || frame_id == INVALID_PAGE_ID, "Invalid Frame ID");
    latch_.lock();
    current_timestamp_++;

    latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; 
}

}  // namespace bustub
