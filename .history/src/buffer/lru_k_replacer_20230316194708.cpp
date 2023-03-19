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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    latch_.lock();
    bool exist = false;
    LRUKNode node = (*(node_store_.begin())).second;
    bool is_inf = (node.history_.size() < k_);
    for(auto p: node_store_){
        LRUKNode cur_node = p.second;
        bool cur_is_inf = cur_node.history_.size() < k_;
        if(is_inf && )
    }
    latch_.unlock();
    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; 
}

}  // namespace bustub
