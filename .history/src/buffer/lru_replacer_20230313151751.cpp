//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> lock(lash);
    if(frame_list.size() == 0){
        return false;
    }
    *frame_id = frame_list.front();
    pos.erase(*frame_id);
    frame_list.pop_front();
    std::lock_guard<std::mutex> unlock(lash);
    return true;

 }

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(lash);
    if(frame_list.size() == 0){
        return;
    }
    *frame_id = frame_list.front();
    pos.erase(*frame_id);
    frame_list.pop_front();
    std::lock_guard<std::mutex> unlock(lash);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {}

auto LRUReplacer::Size() -> size_t { return frame_list.size(); }

}  // namespace bustub
