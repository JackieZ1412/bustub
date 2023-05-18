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
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},lchild_(std::move(left_executor)),rchild_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  lchild_->Init();
  rchild_->Init();
  right_tuples_.clear();
  Tuple tuple{};
  RID rid{};
  bool status = rchild_->Next(&tuple,&rid);
  while(status){
    right_tuples_.emplace_back(tuple);
    status = rchild_->Next(&tuple,&rid);
  }
  status = lchild_->Next(&left_tuple_,&rid);
  iter_ = right_tuples_.begin();
  left_end_ = false;
};
auto NestedLoopJoinExecutor::LeftJoin(Tuple *tuple) -> Tuple{
  std::vector<Value> values{};
  auto left_length = lchild_->GetOutputSchema().GetColumnCount();
  auto right_length = rchild_->GetOutputSchema().GetColumnCount();
  for(decltype(left_length) i = 0; i < left_length; i++){
    values.emplace_back(tuple->GetValue(&lchild_->GetOutputSchema(), i));
  }
  for(decltype(left_length) i = 0; i < right_length; i++){
    values.emplace_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
  }
  return Tuple{values,&GetOutputSchema()};
};

auto NestedLoopJoinExecutor::InnerJoin(Tuple *left_tuple, Tuple *right_tuple) -> Tuple{
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

auto NestedLoopJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
  auto value = plan_->Predicate().EvaluateJoin(left_tuple, lchild_->GetOutputSchema(), right_tuple,
                                               rchild_->GetOutputSchema());

  return !value.IsNull() && value.GetAs<bool>();
};

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  RID lrid{};
  if(plan_->GetJoinType() == JoinType::LEFT){
    // LOG_DEBUG("Enter left join");
    if(iter_ == right_tuples_.end()){
      LOG_DEBUG("right tuple iter ended, check whether the left is end(left join)");
      left_end_ = !(lchild_->Next(&left_tuple_,&lrid));
      if(left_end_){
        LOG_DEBUG("left tuple not exists and end the iter(left join)");
        return false;
      } else {
        LOG_DEBUG("left tuple exists and reset the iter(left join)");
        iter_ = right_tuples_.begin();
      }
    }
    *tuple = LeftJoin(&left_tuple_);
    *rid = tuple->GetRid();
    ++iter_;
    return true;
  } else{
    // LOG_DEBUG("Enter inner join");
    while(true){
      if(iter_ == right_tuples_.end()){
        LOG_DEBUG("right tuple iter ended, check whether the left is end");
        left_end_ = !(lchild_->Next(&left_tuple_,&lrid));
        if(left_end_){
          LOG_DEBUG("left tuple not exists and end the iter");
          return false;
        } else {
          LOG_DEBUG("left tuple exists and reset the iter");
          iter_ = right_tuples_.begin();
        }
      }
      while(iter_ != right_tuples_.end()){
        LOG_DEBUG("Compare the left and right tuples");
        if(Matched(&left_tuple_,&(*iter_))){
          LOG_DEBUG("Left and right tuples are matched, join and output");
          *tuple = InnerJoin(&left_tuple_,&(*iter_));
          *rid = tuple->GetRid();
          ++iter_;
          return true;
        }
        LOG_DEBUG("Not compared, just keep searching");
        ++iter_;
      }
    }
  }
}

}  // namespace bustub
