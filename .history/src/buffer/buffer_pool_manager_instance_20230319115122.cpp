//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
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
      for(int i = 0;i < pages_.size();i++){
        if(pages_[i].GetPinCount() == 0){
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
              page_table_->Remove(it);
              break;
            }
          }
          disk_manager_->WritePage(old_page_id,pages_[frame_id].GetData());
        }
        *page_id = AllocatePage();
        page_table_->Insert(*page_id,frame_id);
        pages_[frame_id].ResetMemory();
        pages_[frame_id].page_id_ = *page_id;
        pages_[frame_id].is_dirty_ = false;
        pages_[frame_id].pin_count_ = 1;
        latch_.unlock(); 
        return &pages_[frame_id];
      }
    }
    page_table_->Insert(*page_id,frame_id);
    latch_.unlock(); 
    return &pages_[frame_id];
  }

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
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
        page_table_->Insert(page_id,frame_id);
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
          page_table_->Insert(page_id,*frame_id);
          pages_[*frame_id].page_id_ = page_id;
          pages_[*frame_id].is_dirty_ = false;
          pages_[*frame_id].pin_count_ = 1;
        }
      }
    }

    latch_.unlock(); 
    return &pages_[page_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  frame_id_t frame_id;
  bool in = (page_table_->Find(page_id,frame_id));
  if(!in || (in && pages_[page_id].GetPinCount() <= 0)){
    latch_.unlock();
    return false;
  }
  pages_[frame_id].pin_count_--;
  if(is_dirty){
    pages_[frame_id].is_dirty_ = true;
  }
  if(pages_[frame_id].GetPinCount() == 0){
    // the page is evictable by the replacer
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  assert(page_id != INVALID_PAGE_ID);
  latch_.lock();
  frame_id_t frame_id;
  bool in = (page_table_->Find(page_id,frame_id));
  if(!in){
    latch_.unlock();
    return false;
  } 
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
  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  latch_.unlock();
  return true; 
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  latch_.lock();
  frame_id_t frame_id;
  bool in = (page_table_->Find(page_id,frame_id));
  if(!in){
    latch_.unlock();
    return true;
  }
  if(pages_[page_id].GetPinCount() > 0){
    latch_.unlock();
    return false;
  }
  page_table_->Remove(page_id);
  pages_[frame_id].ResetMemory();
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  latch_.unlock();
  return false; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
