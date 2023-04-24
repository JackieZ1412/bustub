//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "common/logger.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.push_back(std::make_shared<Bucket>(bucket_size));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  // LOG_DEBUG("Enter IndexOf1");
  int mask = (1 << global_depth_) - 1;
  // LOG_DEBUG("Ready to leave IndexOf1");
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key, const int &depth) -> size_t {
  // LOG_DEBUG("Enter IndexOf2");
  int mask = (1 << depth) - 1;
  // LOG_DEBUG("Ready to leave IndexOf2");
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  latch_.lock();
  // LOG_DEBUG("Enter Hash Table Find");
  size_t index = IndexOf(key);
  bool in = dir_[index]->Find(key,value);
  // LOG_DEBUG("Quit Hash Table Find");
  latch_.unlock();
  return in;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  // LOG_DEBUG("Enter Hash table Remove");
  latch_.lock();
  size_t index = IndexOf(key);
  bool in = dir_[index]->Remove(key);
  // LOG_DEBUG("Quit Hash table remove");
  latch_.unlock();
  return in;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  latch_.lock();
  // LOG_DEBUG("Enter Hash Table Insert");
  // LOG_DEBUG("check all the buckets");
  size_t index = IndexOf(key);
  // LOG_DEBUG("Quit IndexOf");
  // LOG_DEBUG("Ready to enter bucket insert");
  bool isfull = dir_[index]->IsFull();
  if(!isfull){
    // LOG_DEBUG("The bucket is not full");
    dir_[index]->Insert(key,value);
    latch_.unlock();
    return;
  }
  // bool succ = dir_[index]->Insert(key,value);
  if(isfull){
    // LOG_DEBUG("Bucket Insert failed");
    int depth = dir_[index]->GetDepth();
    if(depth == global_depth_){
      // LOG_DEBUG("Need add up global depth");
      // global_depth_++;
      size_t dir_size = dir_.size();
      // LOG_DEBUG("Resize the dir");
      dir_.resize(dir_size * 2);
      // LOG_DEBUG("The dir size and double dir size are: %ld %ld",dir_size,2*dir_size);
      for(size_t i = dir_size; i < dir_size * 2;i++){
        //// LOG_DEBUG("Is this your fault?");
        dir_[i] = dir_[i & ~(1 << global_depth_)];
      }
      global_depth_++;
    } else {
      index = IndexOf(key, depth);
    }
    // get a new bucket
    // LOG_DEBUG("Get a new bucket");
    size_t new_index = index | (1 << depth);
    dir_[new_index] = std::make_shared<Bucket>(bucket_size_,++depth);
    dir_[index]->IncrementDepth();
    num_buckets_++;
    for (size_t i = 0; i < dir_.size(); i++) {
      if ((i & ~(1 << depth)) == new_index) {
        dir_[i] = dir_[new_index];
      }
    }
    // LOG_DEBUG("Remove the items in the old bucket to the new bucket");
    auto items = dir_[index]->GetItems();
    for(auto &item: items){
      size_t n_idx = IndexOf(item.first, depth);
      if(n_idx != index){
        dir_[index]->Remove(item.first);
        dir_[n_idx]->Insert(item.first,item.second);
      }
    }
    latch_.unlock();
    // LOG_DEBUG("Ready to insert new items");
    Insert(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  // LOG_DEBUG("Find an element by key in the bucket");
  for(auto &p: list_){
    if(p.first == key){
      value = p.second;
      // LOG_DEBUG("Successfully Found");
      return true;
    }
  }
  // LOG_DEBUG("didn't found in the bucket");
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  // LOG_DEBUG("Remove an element by key in the bucket");
  for(auto it = list_.begin(); it != list_.end();it++){
    if((*it).first == key){
      list_.erase(it);
      // LOG_DEBUG("Successfully Found and deleted");
      return true;
    }
  }
  // LOG_DEBUG("Delete failed");
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  // LOG_DEBUG("Enter Bucket Insert");
  if(list_.size() == 0){
    // LOG_INFO("The bucket is empty");
    list_.push_back(std::pair<K,V>(key,value));
    return true;
  }
  for(auto &p: list_){
    if(p.first == key){
      p.second = value;
      return true;
    }
  }
  if(list_.size() == size_){
    // the bucket is full
    // LOG_INFO("The bucket is full");
    return false;
  } else {
    list_.push_back(std::pair<K,V>(key,value));
    return true;
  }
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

