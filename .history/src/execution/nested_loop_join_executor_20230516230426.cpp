//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),lchild_(std::move(left_executor)),rchild_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  lchild_->Init();
  rchild_->Init();
  left_tuples_.clear();
  right_tuples_.clear();
  Tuple tuple{};
  RID *rid;
  bool status = lchild_->Next(&tuple,rid);
  while(status){
    left_tuples_.emplace_back(tuple);
    status = lchild_->Next(&tuple,rid);
  }
  status = rchild_->Next(&tuple,rid);
  while(status){
    right_tuples_.emplace_back(tuple);
    status = rchild_->Next(&tuple,rid);
  }
};

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }
  
}  // namespace bustub
