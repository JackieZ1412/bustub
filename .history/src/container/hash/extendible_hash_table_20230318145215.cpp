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

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
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
  size_t index = IndexOf(key);
  bool in = dir_[index]->Find(value);
  latch_.unlock();
  return in;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  latch_.lock();
  size_t index = IndexOf(key);
  bool in = dir_[key]->Remove(key);
  latch_.unlock();
  return in;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  latch_.lock();
  size_t index = IndexOf(key);
  size_t depth = depth_[index]->GetDepth();
  bool succ = dir_[key]->Insert(key);
  if(!succ){
    if(dir_[key].depth_ == global_depth_){
      // global_depth_++;
      dir_.resize(dir_.size() << 1);
      for(size_t i = dir_size; i < dir_size * 2;i++){
        dir_[i] = dir_[i && ~(1 << global_depth_)];
      }
      global_depth_++;
    } else {
      index = IndexOf(key, dir_[index]->GetDepth());
    }
    // get a new bucket
    size_t new_index = index | (1 << dir_[index]->GetDepth());

  }
  latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for(auto &p: list_){
    if(p.first == key){
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for(auto it = list_.begin(); it != list_.end();it++){
    if((*it).first == key){
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  for(auto &p: list_){
    if(p.first == key){
      p.second = value;
      return true;
    }
  }
  if(list_.size() == bucket_size_){
    // the bucket is full
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
