#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {}

void SortExecutor::Init() { 
    child_executor_->Init();
    Tuple child_tuple{};
    RID child_rid;
    bool status = child_executor_->Next(&child_tuple,&child_rid);
    tuples_.clear();
    while(status){
        tuples_.emplace_back(child_tuple);
    }
    std::sort(tuples_.begin(),tuples_.end(),
    [order = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b){
        for(auto &key: order){
            if(key.first == OrderByType::ASC){
                if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareLessThan(key.second->Evaluate(&b, schema)))) {
                    return true;
                } else if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareGreaterThan(key.second->Evaluate(&b, schema)))) {
                    return false;
                }
            } else if(key.first == OrderByType::DESC){
                if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareLessThan(key.second->Evaluate(&b, schema)))) {
                    return true;
                } else if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareGreaterThan(key.second->Evaluate(&b, schema)))) {
                    return false;
                }
            }
        }
    }
    );
    iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(iter_ == tuples_.end()){
        return false;
    } else {
        *tuple = *iter_;
        *rid = tuple->GetRid();
        ++iter_;
    }  
}

}  // namespace bustub
