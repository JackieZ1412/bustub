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
#include "common/logger.h"

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
    pages_[i].ResetMemory();
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
    latch_.lock();
    // LOG_DEBUG("Enter New Page function");
    frame_id_t frame_id = INVALID_PAGE_ID;
    // Page *newpage;
    if(free_list_.empty() == false){
      // LOG_DEBUG("Got free pages");
      // *page_id = AllocatePage();
      frame_id = free_list_.front();
      free_list_.pop_front();
      // LOG_DEBUG("Get a free page from free list");
      // free frame may not need to reset memory?
    } else {
      // LOG_DEBUG("check whether the pages are all pinned");
      bool is_allpined = true;
      for(size_t i = 0;i < pool_size_;i++){
        if(pages_[i].GetPinCount() == 0){
          is_allpined = false;
          break;
        }
      }
      if(is_allpined){
        // LOG_DEBUG("It's all pinned, just return nullptr");
        page_id = nullptr;
        latch_.unlock();
        return nullptr;
      } else {
        // LOG_DEBUG("Not all pages are pinned, select one free page");
        replacer_->Evict(&frame_id); // get a free frame_id of a frame
        if(pages_[frame_id].IsDirty() == true){
          // LOG_DEBUG("The page is dirty, flush it back to the disk");
          page_id_t old_page_id;
          old_page_id = pages_[frame_id].GetPageId();
          disk_manager_->WritePage(old_page_id,pages_[frame_id].GetData());
        }
        page_table_->Remove(pages_[frame_id].page_id_);
        // LOG_DEBUG("Remove the old page from page table");
        // // LOG_DEBUG("Ready to return");
      }
      // // LOG_DEBUG("Go here?");
    }
    // LOG_DEBUG("Initialize the new page");
    *page_id = AllocatePage();
    // LOG_DEBUG("Allocate page successfully");
    // page_table_->Insert(*page_id,frame_id);
    // LOG_DEBUG("Insert to page table successfully");
    pages_[frame_id].ResetMemory();
    // LOG_DEBUG("Reset memory successfully");
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    // LOG_DEBUG("Record access and set evictable");
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page_table_->Insert(*page_id,frame_id);
    disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData());
    // LOG_DEBUG("return the new page");
    latch_.unlock(); 
    return &pages_[frame_id];
  }

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  latch_.lock();
  // std::cout << "Fetching Page " << page_id << std::endl;
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!page_table_->Find(page_id, frame_id)) {
    // Not in memory
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      bool evicted = replacer_->Evict(&frame_id);
      // No page evictable
      if (!evicted) {
        latch_.unlock();
        return nullptr;
      }

      Page *page = &pages_[frame_id];
      if (page->IsDirty()) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
      }
      page_table_->Remove(page->page_id_);
    }

    Page *page = &pages_[frame_id];
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
    page->page_id_ = page_id;
    disk_manager_->ReadPage(page_id, page->data_);
    page_table_->Insert(page_id, frame_id);
  }

  Page *page = &pages_[frame_id];
  page->pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  latch_.unlock();
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  frame_id_t frame_id;
  // LOG_DEBUG("Enter unpin page function");
  bool in = (page_table_->Find(page_id,frame_id));
  // LOG_DEBUG("Successfully found?");
  if(!in || (in && pages_[frame_id].GetPinCount() <= 0)){
    // LOG_DEBUG("Page not found or doesn't get any threads to pin");
    latch_.unlock();
    return false;
  }
  pages_[frame_id].pin_count_--;
  if(is_dirty){
    // LOG_DEBUG("Set dirty bits by is_dirty");
    pages_[frame_id].is_dirty_ = true;
  }
  if(pages_[frame_id].GetPinCount() == 0){
    // the page is evictable by the replacer
    // LOG_DEBUG("Pin count equals to zero, set evictable");
    replacer_->SetEvictable(frame_id,true);
    if(pages_[frame_id].is_dirty_){
      disk_manager_->WritePage(page_id,pages_[frame_id].GetData());
    }
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  assert(page_id != INVALID_PAGE_ID);
  latch_.lock();
  // LOG_DEBUG("Enter flush page function");
  frame_id_t frame_id;
  bool in = (page_table_->Find(page_id,frame_id));
  if(!in){
    // LOG_DEBUG("The page doesn't exist");
    latch_.unlock();
    return false;
  } 
  if(frame_id == INVALID_PAGE_ID){
    // LOG_DEBUG("The page is invalid");
    latch_.unlock();
    return false;
  }
  // LOG_DEBUG("Flush the page");
  disk_manager_->WritePage(page_id,pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  // for(auto it = page_table_.begin();it != page_table_.end();it++){
  //   if((*it).second == frame_id){
  //     page_table_.erase(it);
  //     break;
  //   }
  // }
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
  // LOG_DEBUG("Enter delete page function");
  frame_id_t frame_id;
  bool in = (page_table_->Find(page_id,frame_id));
  if(!in){
    // LOG_DEBUG("The page doesn't exist, return true");
    latch_.unlock();
    return true;
  }
  if(pages_[page_id].GetPinCount() > 0){
    // LOG_DEBUG("The page is pinned, return false");
    latch_.unlock();
    return false;
  }
  // LOG_DEBUG("The page exists and not pinned, ready to delete");
  page_table_->Remove(page_id);
  pages_[frame_id].ResetMemory();
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  latch_.unlock();
  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
