//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{child_executor} {}

void LimitExecutor::Init() { throw NotImplementedException("LimitExecutor is not implemented"); }

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
