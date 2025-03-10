//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  index_info_ = this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
}

void NestIndexJoinExecutor::Init() { 
  child_->Init();
}

auto NestIndexJoinExecutor::NULLLeftJoin(Tuple *tuple) -> Tuple{
  std::vector<Value> values{};
  auto left_length = child_->GetOutputSchema().GetColumnCount();
  auto right_length = plan_->InnerTableSchema().GetColumnCount();
  for(decltype(left_length) i = 0; i < left_length; i++){
    values.emplace_back(tuple->GetValue(&child_->GetOutputSchema(), i));
  }
  for(decltype(left_length) i = 0; i < right_length; i++){
    values.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
  }
  return Tuple{values,&GetOutputSchema()};
};

auto NestIndexJoinExecutor::InnerJoin(Tuple *left_tuple, Tuple *right_tuple) -> Tuple{
  std::vector<Value> values{};
  auto left_length = child_->GetOutputSchema().GetColumnCount();
  auto right_length = plan_->InnerTableSchema().GetColumnCount();
  for(decltype(left_length) i = 0; i < left_length; i++){
    values.emplace_back(left_tuple->GetValue(&child_->GetOutputSchema(), i));
  }
  for(decltype(left_length) i = 0; i < right_length; i++){
    values.emplace_back(right_tuple->GetValue(&plan_->InnerTableSchema(), i));
  }
  return Tuple{values,&GetOutputSchema()};
};

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  Transaction *txn = GetExecutorContext()->GetTransaction();
  Tuple left_tuple{};
  Tuple right_tuple{};
  std::vector<Value> values;
  
}

}  // namespace bustub
