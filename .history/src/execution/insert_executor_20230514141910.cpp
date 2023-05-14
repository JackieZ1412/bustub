//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_(std::move(child_executor)) {
        this->checking_table_ = this->exec_ctx_->GetCatalog()->GetTable(plan->table_oid_);
        this->index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(checking_table_->name_);
    }

void InsertExecutor::Init() { 
    iter_ = checking_table_->table_->Begin(exec_ctx_->GetTransaction());
    child_executor_->Init();
    at_end_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple child_tuple{}; 
    if(at_end_){
        return false;
    }
    bool status = child_executor_->Next(&child_tuple, rid);
    while(!status){
        // tuple is not the end
        Transaction *txn;
        txn = exec_ctx_->GetTransaction();
        checking_table_->table_->InsertTuple(child_tuple,rid,txn);
        for(auto &temp : index_info_){
            temp->index_->InsertEntry(child_tuple,*rid,txn);
        }
        status = child_executor_->Next(&child_tuple,rid);
    }
    at_end_ = true;
    return true;
}
    
}  // namespace bustub
