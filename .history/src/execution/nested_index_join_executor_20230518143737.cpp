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
}

void NestIndexJoinExecutor::Init() { throw NotImplementedException("NestIndexJoinExecutor is not implemented"); }

auto NestIndexJoinExecutor::NULLLeftJoin(Tuple *tuple) -> Tuple{
  std::vector<Value> values{};
  auto left_length = child_->GetOutputSchema().GetColumnCount();
  auto right_length = plan_->InnerTableSchema().GetColumnCount();
  for(decltype(left_length) i = 0; i < left_length; i++){
    values.emplace_back(left_tuple->GetValue(&child_->GetOutputSchema(), i));
  }
  for(decltype(left_length) i = 0; i < right_length; i++){
    values.emplace_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
  }
  return Tuple{values,&GetOutputSchema()};
};

auto NestIndexJoinExecutor::InnerJoin(Tuple *left_tuple, Tuple *right_tuple) -> Tuple{
  std::vector<Value> values{};
  auto left_length = lchild_->GetOutputSchema().GetColumnCount();
  auto right_length = rchild_->GetOutputSchema().GetColumnCount();
  for(decltype(left_length) i = 0; i < left_length; i++){
    values.emplace_back(left_tuple->GetValue(&lchild_->GetOutputSchema(), i));
  }
  for(decltype(left_length) i = 0; i < right_length; i++){
    values.emplace_back(right_tuple->GetValue(&rchild_->GetOutputSchema(), i));
  }
  return Tuple{values,&GetOutputSchema()};
};

auto NestIndexJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
  auto value = plan_->Predicate().EvaluateJoin(left_tuple, lchild_->GetOutputSchema(), right_tuple,
                                               rchild_->GetOutputSchema());

  return !value.IsNull() && value.GetAs<bool>();
};
auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
