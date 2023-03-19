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
        if(frame_iter->second.GetKDist(k_) == -1 && it->second.GetKDist(k_) == -1){
            if(frame_iter->second.GetTimeStamp()[0] > it->second.GetTimeStamp()[0]){
                frame_iter = it;
            }
        } else if(frame_iter->second.GetKDist(k_) != -1 && it->second.GetKDist(k_) == -1){
            frame_iter = it;
        } else if(frame_iter->second.GetKDist(k_) != -1 && it->second.GetKDist(k_) != -1 && frame_iter->second.GetKDist(k_) > it->second.GetKDist(k_)){
            frame_iter = it;
        }
    }
    *frame_id = frame_iter->first;
    evictable_frames.erase(frame_iter);
    frames_map.erase(*frame_id);
    latch_.unlock();
    return true;

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_ || frame_id == INVALID_PAGE_ID, "Invalid Frame ID");
    latch_.lock();
    current_timestamp_++;
    bool in_map = (frames_map.find(frame_id) != frames_map.end());
    bool in_evict_list = (evictable_frames.find(frame_id) != evictable_frames.end());
    if(!in_map && !in_evict_list){
        frames_map.insert({frame_id,FrameNode(frame_id)});
        frames_map[frame_id].RecordAccess(current_timestamp_);
    } else if(in_map && !in_evict_list){
        frames_map[frame_id].RecordAccess(current_timestamp_);
    } else if(in_map && in_evict_list){
        frames_map[frame_id].RecordAccess(current_timestamp_);
        evictable_frames[frame_id].RecordAccess(current_timestamp_);
    } else if(!in_map && in_evict_list){
        // impossible (?)
        evictable_frames[frame_id].RecordAccess(current_timestamp_);
    }
    latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    latch_.lock();
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_ || frame_id == INVALID_PAGE_ID, "Invalid Frame ID");
    current_timestamp_++;
    bool in_map = (frames_map.find(frame_id) != frames_map.end());
    bool in_evict_list = (evictable_frames.find(frame_id) != evictable_frames.end());
    if(in_evict_list && !set_evictable){
        evictable_frames.erase(frame_id);
    } else if(!in_evict_list && set_evictable){
        if(in_map){
            auto node = frames_map[frame_id];
            evictable_frames.insert({frame_id,node});
        }
    }
    latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    latch_.lock();
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_ || frame_id == INVALID_PAGE_ID, "Invalid Frame ID");
    BUSTUB_ASSERT(evictable_frames.find(frame_id) == evictable_frames.end(), "Evicting non-evictable frame");
    latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { 
    return evictable_frames.size(); 
}

}  // namespace bustub
