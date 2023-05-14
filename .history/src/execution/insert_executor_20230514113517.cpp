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
    : AbstractExecutor(exec_ctx),plan_{plan} {
        this->checking_table_ = this->exec_ctx_->GetCatalog()->GetTable(plan->table_oid_);
        this->index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(checking_table_->name_);
    }

void InsertExecutor::Init() { throw NotImplementedException("InsertExecutor is not implemented"); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
