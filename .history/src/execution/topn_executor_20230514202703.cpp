#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {}

void TopNExecutor::Init() { 
    
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
