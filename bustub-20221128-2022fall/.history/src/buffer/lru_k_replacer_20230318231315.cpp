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
            
        }
    }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; 
}

}  // namespace bustub
