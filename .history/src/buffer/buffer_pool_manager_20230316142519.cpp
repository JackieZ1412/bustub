//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
    latch_.lock();
    frame_id_t frame_id;
    // Page *newpage;
    if(free_list_.empty() == false){
      *page_id = AllocatePage();
      frame_id = free_list_.front();
      free_list_.pop_front();
      // free frame may not need to reset memory?
      pages_[frame_id].ResetMemory();
      pages_[frame_id].page_id_ = *page_id;
      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].pin_count_ = 0;
    } else {
      bool is_allpined = true;
      for(auto p: page_table_){
        if(pages_[p.second].GetPinCount() == 0){
          is_allpined = false;
          break;
        }
      }
      if(is_allpined){
        latch_.unlock();
        return nullptr;
      } else {
        replacer_->Victim(&frame_id); // get a free frame_id of a frame
        if(pages_[frame_id].IsDirty() == true){
          page_id_t old_page_id;
          for(auto it = page_table_.begin();it != page_table_.end(); it++){
            if((*it).second == frame_id){
              old_page_id = (*it).first;
              page_table_.erase(it);
              break;
            }
          }
          disk_manager_->WritePage(old_page_id,pages_[frame_id].GetData());
        }
        *page_id = AllocatePage();
        page_table_.emplace(page_id,frame_id);
        pages_[frame_id].ResetMemory();
        pages_[frame_id].page_id_ = *page_id;
        pages_[frame_id].is_dirty_ = false;
        pages_[frame_id].pin_count_ = 1;
      }
    }
    page_table_.emplace(*page_id,frame_id);
    latch_.unlock(); 
    return &pages_[frame_id];
  }

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
    latch_.lock();
    bool in = (page_table_.find(page_id) != page_table_.end());
    if(!in){
      // check if all pages are not evictable(pinned)
      if(free_list_.empty() != true){
        auto frame_id = free_list_.front();
        free_list_.pop_front();
        disk_manager_->ReadPage(page_id,pages_[frame_id].GetData());
        if(pages_[frame_id].GetData()[0] == '\0'){
          // clean page that means can't find page
          latch_.unlock();
          return nullptr;
        }
        page_table_.emplace(page_id,frame_id);
      } else {
        auto is_allpined = true;
        for(auto p: page_table_){
          if(pages_[p.second].GetPinCount() == 0){
            is_allpined = false;
            break;
          }
        }
        if(is_allpined){
          // all frames are pinned
          latch_.unlock();
          return nullptr;
        } else {
          frame_id_t *frame_id;
          replacer_->Victim(frame_id); // get a free frame_id of a frame
          if(pages_[*frame_id].IsDirty() == true){
            page_id_t old_page_id;
            for(auto it = page_table_.begin();it != page_table_.end(); it++){
              if((*it).second == *frame_id){
                old_page_id = (*it).first;
                page_table_.erase(it);
                break;
              }
            }
            disk_manager_->WritePage(old_page_id,pages_[*frame_id].GetData());
          }
          page_table_.emplace(page_id,*frame_id);
          pages_[*frame_id].page_id_ = page_id;
          pages_[*frame_id].is_dirty_ = false;
          pages_[*frame_id].pin_count_ = 1;
        }
      }
    }

    latch_.unlock(); 
    return &pages_[page_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  bool in = (page_table_.find(page_id) != page_table_.end());
  if(!in || (in && pages_[page_id].GetPinCount() <= 0)){
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  pages_[frame_id].pin_count_--;
  if(pages_[frame_id].GetPinCount() == 0){
    // the page is evictable by the replacer
    replacer_->Unpin(frame_id);
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  assert(page_id != INVALID_PAGE_ID);
  latch_.lock();
  bool in = (page_table_.find(page_id) != page_table_.end());
  if(!in){
    latch_.unlock();
    return false;
  } 
  frame_id_t frame_id = page_table_[page_id];
  if(frame_id == INVALID_PAGE_ID){
    latch_.unlock();
    return false;
  }
  disk_manager_->WritePage(page_id,pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  // for(auto it = page_table_.begin();it != page_table_.end();it++){
  //   if((*it).second == frame_id){
  //     page_table_.erase(it);
  //     break;
  //   }
  // }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true; 
}

void BufferPoolManager::FlushAllPages() {
  for(auto p: page_table_){
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
