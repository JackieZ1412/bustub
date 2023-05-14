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
    while(status){
        tuples_.emplace_back(child_tuple);
    }

    iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(iter_ == tuples_.end()){
        return false;
    } else {
        *tuple = *iter;
        
    }
}

}  // namespace bustub
