#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_latch_.RLock();
  // LOG_DEBUG("root latch rlocked");
  auto page = FindLeafPage(key,false,false,0,transaction);
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  auto in = leaf->Lookup(key,&value,comparator_);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPgImp(page->GetPageId(), false);
  if(!in){
    return false;
  } else {
    result->push_back(value);
    return true;
  }
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_DEBUG("Insert in B Plus Tree");
  // LOG_DEBUG("Before insertion, the shape of the tree is");
  // Because we got root latch, so add to page set a nullptr to represent root
  root_latch_.WLock();
  // LOG_DEBUG("root latch wlocked");
  transaction->AddIntoPageSet(nullptr);
  // LOG_DEBUG("Broke here?");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  // LOG_DEBUG("Or broke here?");
  if(IsEmpty()){
    // LOG_DEBUG("Is it?");
    StartNewTree(key,value);
    // LOG_DEBUG("Can realease the latches");
    TopDownRelease(transaction);
    // LOG_DEBUG("After insertion, the shape of the tree is");
    // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
    // auto *root = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPgImp(root_page_id_));
    // std::cout << "TMD where are you bitch???" << root->GetSize() <<std::endl;
    // for(int i = 0;i < root->GetSize();i++){
    //   std::cout << "value is " << value << std::endl;
    // }
    return true;
  }
  // LOG_DEBUG("Or it?");
  bool res = InsertIntoLeaf(key,value,transaction);
  // LOG_DEBUG("After insertion, the shape of the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // LOG_DEBUG("StartNewTree function");
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPgImp(&page_id);
  if(page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  root_page_id_ = page_id;
  // LOG_DEBUG("Init leaf node and insert k&v");
  auto *node = reinterpret_cast<LeafPage *>(page->GetData());
  node->Init(page_id,INVALID_PAGE_ID,leaf_max_size_);
  node->Insert(key,value,comparator_);
  buffer_pool_manager_->UnpinPgImp(page_id,true);
  // LOG_DEBUG("quit StartNewTree function");
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // LOG_DEBUG("Insert into leaf function");
  auto *page = FindLeafPage(key, false,false,1,transaction);
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  // std::cout << "The max size and leaf size are" << leaf_max_size_ << " and " << leaf->GetSize() << std::endl;
  ValueType temp;
  if(leaf->Lookup(key,&temp,comparator_)){
    // LOG_DEBUG("duplicate key and can release latch");
    TopDownRelease(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
  // auto size = leaf->GetSize();
  // LOG_DEBUG("Before insertion, the leaf is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(leaf->GetPageId())),buffer_pool_manager_);
  auto new_size = leaf->Insert(key,value,comparator_);
  // LOG_DEBUG("The size and new size are: %d and %d",size,new_size);
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(leaf->GetPageId())),buffer_pool_manager_);
  // std::cout << "The new size is " << new_size << std::endl;
  // std::cout << "The key is " << key << " and the value is " << value << std::endl;
  if(new_size == leaf_max_size_){
    // LOG_DEBUG("The leaf is full and need to split so we can NOT release latch");
    auto new_leaf = Split(leaf);
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_leaf->GetPageId());
    auto new_key = new_leaf->KeyAt(0);
    InsertIntoParent(leaf,new_key,new_leaf,transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPgImp(leaf->GetPageId(),true);
    buffer_pool_manager_->UnpinPgImp(new_leaf->GetPageId(),true);
    return true;
  } else if (new_size < leaf_max_size_){
    // LOG_DEBUG("The leaf is not full and insert successfully");
    // LOG_DEBUG("Can release latch");
    TopDownRelease(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // LOG_DEBUG("Split function");
  page_id_t page_id;
  auto new_page = buffer_pool_manager_->NewPgImp(&page_id);
  if(new_page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    return nullptr;
  }
  auto new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());
  if(node->IsLeafPage()){
    // LOG_DEBUG("Leaf page split");
    auto *lpage = reinterpret_cast<LeafPage *>(node);
    // LOG_DEBUG("New leaf page init");
    auto *new_lpage = reinterpret_cast<LeafPage *>(new_node);
    new_lpage->Init(new_page->GetPageId(),node->GetParentPageId(), leaf_max_size_);
    // LOG_DEBUG("Get ready to enter move half");
    lpage->MoveHalfTo(new_lpage);
  } else {
    // LOG_DEBUG("Internal page split");
    auto *ipage = reinterpret_cast<InternalPage *>(node);
    // LOG_DEBUG("New internal page init");
    auto *new_ipage = reinterpret_cast<InternalPage *>(new_node);
    new_ipage->Init(new_page->GetPageId(),node->GetParentPageId(),internal_max_size_);
    ipage->MoveHalfTo(new_ipage,buffer_pool_manager_);
  }
  // for(int i = 0;i < node->GetSize();i++){
  //   auto temp = node->GetItem(i);
  //   std::cout << "In node, the key is " << temp.first << " value is " << temp.second << std::endl;
  // }
  // for(int i = 0;i < new_node->GetSize();i++){
  //   auto temp = new_node->GetItem(i);
  //   std::cout << "In node, the key is " << temp.first << " value is " << temp.second << std::endl;
  // }
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // LOG_DEBUG("InsertIntoParent function");
  if(old_node->IsRootPage()){
    // LOG_DEBUG("old node is root page");
    // page_id_t page_id;
    auto page = buffer_pool_manager_->NewPgImp(&root_page_id_);
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    // root_page_id_ = page_id;
    // std::cout << page_id << std::endl;
    new_root->Init(root_page_id_,INVALID_PAGE_ID,internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    // TopDownRelease(transaction);
    buffer_pool_manager_->UnpinPgImp(page->GetPageId(),true);
    UpdateRootPageId(0);
    
    TopDownRelease(transaction);
    return;
  } else{
    // LOG_DEBUG("not root page");
    page_id_t parent_pid = old_node->GetParentPageId();
    auto *parent_pg = buffer_pool_manager_->FetchPgImp(parent_pid);
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_pg->GetData());
    if(parent_node->GetSize() < internal_max_size_){
      // LOG_DEBUG("The internal parent page is not full need not split so we can release latch");
      parent_node->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
      TopDownRelease(transaction);
      buffer_pool_manager_->UnpinPgImp(parent_pid,true);
      // // ToString(parent_node,buffer_pool_manager_);
      return;
    } else if(parent_node->GetSize() == internal_max_size_){
      // LOG_DEBUG("The internal parent page is full");
      // Here needs a special split that split from the place where old node exists
      auto new_inode = Split(parent_node);
      // // ToString(parent_node,buffer_pool_manager_);
      // // ToString(new_inode,buffer_pool_manager_);
      // std::cout << key << " and " << new_inode->KeyAt(0) << std::endl;
      if(comparator_(key,new_inode->KeyAt(0)) > 0){
        // LOG_DEBUG("Case 1");
        new_inode->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
        old_node->SetParentPageId(new_inode->GetPageId());
        new_node->SetParentPageId(new_inode->GetPageId());
      } else {
        // LOG_DEBUG("Case 2");
        parent_node->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
        old_node->SetParentPageId(parent_node->GetPageId());
        new_node->SetParentPageId(parent_node->GetPageId());
      }
      // LOG_DEBUG("iterative insertion into parent node");
      InsertIntoParent(parent_node,new_inode->KeyAt(0),new_inode,transaction);
      buffer_pool_manager_->UnpinPgImp(parent_pg->GetPageId(),true);
      buffer_pool_manager_->UnpinPgImp(new_inode->GetPageId(),true);
    }

  }
  return;
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // LOG_DEBUG("Remove function");
  // LOG_DEBUG("Before deletion, the shape of the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  root_latch_.WLock();
  // LOG_DEBUG("root latch wlocked");
  transaction->AddIntoPageSet(nullptr);
  if(IsEmpty()){
    // LOG_DEBUG("It is an empty tree and release and return");
    TopDownRelease(transaction);
    return;
  }
  auto page = FindLeafPage(key,false,false,2,transaction);
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  auto temp = leaf->GetSize();
  auto size = leaf->RemoveAndDeleteRecord(key,comparator_);
  if(temp == size){
    // LOG_DEBUG("Not deleted");
    TopDownRelease(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPgImp(page->GetPageId(),false);
    return;
  }
  bool cor = false;
  if(size < leaf->GetMinSize()){
    // LOG_DEBUG("less than min size and might need to split");
    cor = CoalesceOrRedistribute(leaf,transaction);
    page->WUnlatch();
    if(cor){
      transaction->AddIntoDeletedPageSet(leaf->GetPageId());
    }
    // buffer_pool_manager_->UnpinPgImp(page->GetPageId(),true);
  } else {
    TopDownRelease(transaction);
    page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPgImp(leaf->GetPageId(),true);
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
  transaction->GetDeletedPageSet()->clear();
  // LOG_DEBUG("After Remove, the shape of the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // LOG_DEBUG("Coalesce or Redistribute function");
  if(node->IsRootPage()){
    // LOG_DEBUG("Root page condition");
    TopDownRelease(transaction);
    return AdjustRoot(node);
  }
  if(node->GetSize() >= node->GetMinSize()){
    TopDownRelease(transaction);
    return false;
  }
  // LOG_DEBUG("Here prepare to find a prev node");
  auto *page = buffer_pool_manager_->FetchPgImp(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(page->GetData());
  auto index = parent_node->ValueIndex(node->GetPageId());
  if(index == 0){
    if(index == parent_node->GetSize() - 1){
      return false;
    }
    // LOG_DEBUG("The node is first child with no prev sibling");
    auto *sibling_page = buffer_pool_manager_->FetchPgImp(parent_node->ValueAt(index + 1));
    sibling_page->WLatch();
    auto *sibling = reinterpret_cast<N *>(sibling_page);
    if(node->GetSize() + sibling->GetSize() <= node->GetMaxSize()){
      // LOG_DEBUG("Can coalesce");
      auto rm_index = parent_node->ValueIndex(node->GetPageId());
      bool parent_delete = Coalesce(sibling,node,parent_node,rm_index,transaction);
      transaction->AddIntoDeletedPageSet(sibling->GetPageId());
      if(parent_delete){
        transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
      }
      buffer_pool_manager_->UnpinPgImp(parent_node->GetPageId(),true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPgImp(sibling->GetPageId(),true);
      return false;
    } else {
      // LOG_DEBUG("Can't coalesce and ready to redistribute");
      auto rm_index = parent_node->ValueIndex(node->GetPageId());
      Redistribute(sibling,node,parent_node,rm_index);
      TopDownRelease(transaction);
      buffer_pool_manager_->UnpinPgImp(parent_node->GetPageId(),true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPgImp(sibling->GetPageId(),true);
      return false;
    }
  } else {
    // LOG_DEBUG("The node is not first child and have prev sibling");
    auto *sibling_page = buffer_pool_manager_->FetchPgImp(parent_node->ValueAt(index - 1));
    sibling_page->WLatch();
    auto *sibling = reinterpret_cast<N *>(sibling_page->GetData());
    if(node->GetSize() + sibling->GetSize() <= node->GetMaxSize()){
      // LOG_DEBUG("Can coalesce");
      auto rm_index = parent_node->ValueIndex(node->GetPageId());
      bool parent_delete = Coalesce(sibling,node,parent_node,rm_index,transaction);
      if(parent_delete){
        transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
      }
      buffer_pool_manager_->UnpinPgImp(parent_node->GetPageId(),true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPgImp(sibling->GetPageId(),true);
      return true;
    } else {
      // LOG_DEBUG("Can't coalesce and ready to redistribute");
      auto rm_index = parent_node->ValueIndex(node->GetPageId());
      Redistribute(sibling,node,parent_node,rm_index);
      TopDownRelease(transaction);
      buffer_pool_manager_->UnpinPgImp(parent_node->GetPageId(),true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPgImp(sibling->GetPageId(),true);
      return false;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  // LOG_DEBUG("Coalesce function");
  // LOG_DEBUG("Before Coalesce the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  if(node->IsLeafPage()){
    // LOG_DEBUG("Leaf page");
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *sibling = reinterpret_cast<LeafPage *>(neighbor_node);
    auto next_pid = sibling->GetNextPageId();
    leaf->MoveAllTo(sibling);
    if(index == 0){
      sibling->SetNextPageId(next_pid);
    }
    parent->Remove(index);
  } else {
    // LOG_DEBUG("internal page");
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *sibling = reinterpret_cast<InternalPage *>(neighbor_node);
    if(index == 0){
      // LOG_DEBUG("coalesce case 1");
      sibling->MoveAllTo(internal,parent->KeyAt(index + 1),buffer_pool_manager_);
      parent->Remove(index + 1);
    } else{
      // LOG_DEBUG("coalesce case 2");
      internal->MoveAllTo(sibling,parent->KeyAt(index),buffer_pool_manager_);
      parent->Remove(index);
    }
  }
  // LOG_DEBUG("after Coalesce the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  return CoalesceOrRedistribute(parent, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, 
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,int index) {
  // LOG_DEBUG("Redistribute function");
  // LOG_DEBUG("Before Redistribute the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  if(node->IsLeafPage()){
    // LOG_DEBUG("Leaf page");
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *sibling = reinterpret_cast<LeafPage *>(neighbor_node);
    if(index == 0){
      sibling->MoveFirstToEndOf(leaf);
      parent->SetKeyAt(index + 1, sibling->KeyAt(0));
    } else {
      sibling->MoveLastToFrontOf(leaf);
      parent->SetKeyAt(index,node->KeyAt(0));
    }
  } else {
    // LOG_DEBUG("internal page");
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *sibling = reinterpret_cast<InternalPage *>(neighbor_node);
    if(index == 0){
      sibling->MoveFirstToEndOf(internal,parent->KeyAt(index + 1),buffer_pool_manager_);
      parent->SetKeyAt(index + 1, sibling->KeyAt(0));
    } else {
      sibling->MoveLastToFrontOf(internal,parent->KeyAt(index),buffer_pool_manager_);
      parent->SetKeyAt(index,node->KeyAt(0));
    }
  }
  // LOG_DEBUG("After Redistribute the tree is");
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { 
  // LOG_DEBUG("Adjusting root");
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1){
    // LOG_DEBUG("case 1");
    // LOG_DEBUG("make the only child root");
    auto *root = reinterpret_cast<InternalPage *>(old_root_node);
    auto *page = buffer_pool_manager_->FetchPgImp(root->ValueAt(0));
    auto *newroot = reinterpret_cast<BPlusTreePage *>(page->GetData());
    root_page_id_=  page->GetPageId();
    newroot->SetPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPgImp(root_page_id_,true);
    UpdateRootPageId(0);
    return true;
  } else if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){
    // LOG_DEBUG("case 2 and set the tree empty");
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost,bool rightMost,int op, Transaction *transaction) {
  // For op, 0 represents search; 1 represents insert; 2 represents delete
  // LOG_DEBUG("FindLeafPage function");
  assert(op == 0 ? !(leftMost && rightMost) : transaction != nullptr);
  assert(root_page_id_ != INVALID_PAGE_ID);
  auto root = buffer_pool_manager_->FetchPgImp(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(root->GetData());
  if(op == 0){
    root_latch_.RUnlock();
    // LOG_DEBUG("root latch runlocked");
    root->RLatch();
  } else {
    root->WLatch();
    if(op == 2 && node->GetSize() > 2){
      TopDownRelease(transaction);
    }
    if(op == 1 && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1){
      TopDownRelease(transaction);
    }
    if(op == 1 && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()){
      TopDownRelease(transaction);
    }
  }
  while(node->IsLeafPage() == false){
    auto *temp = reinterpret_cast<InternalPage *>(node);
    page_id_t page_id;
    if(leftMost){
      page_id = temp->ValueAt(0);

    } else if(rightMost) {
      page_id = temp->ValueAt(temp->GetSize() - 1);
    }else{
      page_id = temp->Lookup(key,comparator_);
    }
    assert(page_id > 0);
    auto new_page = buffer_pool_manager_->FetchPage(page_id);
    auto new_node = reinterpret_cast<BPlusTreePage *>(new_page->GetData());

    if(op == 0){
      new_page->RLatch();
      root->RUnlatch();
      buffer_pool_manager_->UnpinPgImp(root->GetPageId(), false);
    } else if(op == 1){
      new_page->WLatch();
      transaction->AddIntoPageSet(root);

      if(new_node->IsLeafPage() && new_node->GetSize() < new_node->GetMaxSize() - 1){
        // LOG_DEBUG("Safe and release top down");
        TopDownRelease(transaction);
      }
      if(!new_node->IsLeafPage() && new_node->GetSize() < new_node->GetMaxSize()){
        // LOG_DEBUG("Safe and release top down");
        TopDownRelease(transaction);
      }
    } else if(op == 2){
      new_page->WLatch();
      transaction->AddIntoPageSet(root);

      if(new_node->GetSize() > new_node->GetMinSize()){
        // LOG_DEBUG("Safe and release top down");
        TopDownRelease(transaction);
      }
    }
    // buffer_pool_manager_->UnpinPgImp(node->GetPageId(),false);
    // node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(page_id));
    root = new_page;
    node = new_node;
  }
  // buffer_pool_manager_->UnpinPgImp(node->GetPageId(),false);
  // auto pid = node->GetPageId();
  // auto page = buffer_pool_manager_->FetchPgImp(pid);
  return root;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::TopDownRelease(Transaction * transaction) {
  // LOG_DEBUG("Top down release the latches that this transaction holds");
  // LOG_DEBUG("No while?");
  // std::cout << transaction->GetPageSet()->size() << std::endl;
  while (!transaction->GetPageSet()->empty()) {
    // LOG_DEBUG("Hello?");
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      // LOG_DEBUG("nullptr means root latch");
      this->root_latch_.WUnlock();
      // LOG_DEBUG("root latch wunlocked");
    } else {
      page->WUnlatch();
      // LOG_DEBUG("Now the page id is %d", page->GetPageId());
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
  // LOG_DEBUG("finished while");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  // LOG_DEBUG("Iteration begin with no key");
  if (root_page_id_ == INVALID_PAGE_ID) {
    // LOG_DEBUG("Empty tree got no iterators");
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  // LOG_DEBUG("root latch rlocked");
  auto *leaf_page = FindLeafPage(KeyType(),true,false,0,nullptr);
  // LOG_DEBUG("Is the leaf the left most?");
  // std::cout << "The page id of the leaf is " << leaf_page->GetPageId() << std::endl;
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  // LOG_DEBUG("Iteration begin with a key");
  // std::cout << "The key is " << key << std::endl;
  if (root_page_id_ == INVALID_PAGE_ID) {
    // LOG_DEBUG("Empty tree got no iterators");
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  // LOG_DEBUG("root latch rlocked");
  auto *leaf_page = FindLeafPage(key,false,false,0,nullptr);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto index = leaf->KeyIndex(key,comparator_);
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  // LOG_DEBUG("The index is %d",index);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  // LOG_DEBUG("Iteration end with no key");
  if (root_page_id_ == INVALID_PAGE_ID) {
    // LOG_DEBUG("Empty tree got no iterators");
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  // LOG_DEBUG("root latch rocked");
  auto *leaf_page = FindLeafPage(KeyType(),false,true,0,nullptr);
  // LOG_DEBUG("Is the leaf the right most?");
  // std::cout << "The page id of the leaf is " << leaf_page->GetPageId() << std::endl;
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto index = leaf->GetSize();
  // ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPgImp(root_page_id_)),buffer_pool_manager_);
  // LOG_DEBUG("The index is %d",index);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  root_latch_.RLock();
  root_latch_.RUnlock();
  return root_page_id_; 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  // ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
