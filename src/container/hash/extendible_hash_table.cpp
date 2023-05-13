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
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("Enter Hash Table Find");
  size_t index = IndexOf(key);
  bool in = dir_[index]->Find(key,value);
  // LOG_DEBUG("Quit Hash Table Find");
  // latch_.unlock();
  return in;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  // LOG_DEBUG("Enter Hash table Remove");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  bool in = dir_[index]->Remove(key);
  // LOG_DEBUG("Quit Hash table remove");
  // latch_.unlock();
  return in;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {
    auto index = IndexOf(key);
    auto target_bucket = dir_[index];

    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);
      for (int i = 0; i < capacity; i++) {
        dir_[i + capacity] = dir_[i];
      }
    }

    int mask = 1 << target_bucket->GetDepth();
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);

    for (const auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }

    num_buckets_++;

    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = bucket_1;
        } else {
          dir_[i] = bucket_0;
        }
      }
    }
  }

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  for (auto &item : target_bucket->GetItems()) {
    if (item.first == key) {
      item.second = value;
      return;
    }
  }

  target_bucket->Insert(key, value);
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

