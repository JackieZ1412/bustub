#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {}

void SortExecutor::Init() { 
    child_executor_->Init();
    
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
