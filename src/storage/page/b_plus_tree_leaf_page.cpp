//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "common/logger.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const { 
  // LOG_DEBUG("Enter key index function");
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
    return comparator(pair.first, k) < 0;
  });
  return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  return array_[index];
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int {
  // LOG_DEBUG("Enter insert in b plus tree leaf page");
  int idx = KeyIndex(key,comparator); //first larger than key
  assert(idx >= 0);
  IncreaseSize(1);
  int curSize = GetSize();
  for (int i = curSize - 1; i > idx; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[idx].first = key;
  array_[idx].second = value;
  return curSize;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // TODO: Implement after Copy function
  // LOG_DEBUG("Enter move half to function in leaf page");
  int start_split_indx = GetMinSize();
  SetSize(start_split_indx);
  recipient->CopyNFrom(array_ + start_split_indx, GetMaxSize() - start_split_indx);
  // LOG_DEBUG("Finished move half");
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // LOG_DEBUG("Copy N function from leaf page");
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
  // LOG_DEBUG("finish copy N from leaf page");
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  // LOG_DEBUG("Enter lookup function in b plus tree leaf page");
  for(int i = 0;i < GetSize();i++){
    if(comparator(key,array_[i].first) == 0){
      // LOG_DEBUG("Successfully found with index %d",i);
      *value = array_[i].second;
      return true;
    }
  }
  // LOG_DEBUG("Didn't found ");
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) { 
  // LOG_DEBUG("Enter RemoveAndDeleteRecord function in b plus tree leaf page");
  int index = KeyIndex(key,comparator);
  // LOG_DEBUG("The index is %d",index);
  // std::cout << "The two params are " << array_[index].first << " and " << key << std::endl;
  if(index == GetSize() || comparator(array_[index].first, key) != 0){
    // LOG_DEBUG("Didn't found");
    return GetSize();
  }
  // LOG_DEBUG("Key found");
  std::move(array_ + index + 1,array_ + GetSize(),array_ + index);
  IncreaseSize(-1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  auto first_item = GetItem(0);
  std::move(array_ + 1,array_ + GetSize(),array_);
  IncreaseSize(-1);
  recipient->CopyLastFrom(first_item);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  auto last_item = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(last_item);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  std::move(array_,array_ + GetSize(), array_ + 1);
  array_[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
