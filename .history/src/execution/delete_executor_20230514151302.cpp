//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_(std::move(child_executor)) {
        this->checking_table_ = this->exec_ctx_->GetCatalog()->GetTable(plan->table_oid_);
        this->index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(checking_table_->name_);
    }

void DeleteExecutor::Init() { 
    child_executor_->Init();
    at_end_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    Tuple child_tuple{}; 
    int32_t count = 0;
    if(at_end_){
        return false;
    }
    bool status = child_executor_->Next(&child_tuple,rid);
    while(status){
        Transaction *txn;
        txn = exec_ctx_->GetTransaction();
        checking_table_->table_->MarkDelete(rid,txn);
        for(auto &temp : index_info_){
            temp->index_->DeleteEntry(child_tuple,*rid,txn);
        }
        status = child_executor_->Next(&child_tuple,rid);
        count++;
    }
}

}  // namespace bustub
