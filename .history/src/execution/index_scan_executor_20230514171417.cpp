//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
     : AbstractExecutor(exec_ctx),plan_{plan},iter_{BPlusTreeIndexIteratorForOneIntegerColumn(nullptr, nullptr)} {
        this->checking_index_ = this->exec_ctx_->GetCatalog()->GetIndex(plan->index_oid_);
        this->checking_table_ = this->exec_ctx_->GetCatalog()->GetTable(checking_index_->table_name_);
        this->tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(checking_index_->index_.get());
        this->iter_ = tree_->GetBeginIterator();
    }

void IndexScanExecutor::Init() { 
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(iter_ == tree_->GetEndIterator()){
        return false;
    } else {
        *rid = (*iter_).second;
        auto result = checking_table_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
        ++iter_;

        return result;
    }
}

}  // namespace bustub
