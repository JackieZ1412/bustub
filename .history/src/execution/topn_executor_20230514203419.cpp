#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {}

void TopNExecutor::Init() { 
    child_executor_->Init();
    Tuple child_tuple{};
    RID child_rid;
    auto cmp = [order = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b){
        for(auto &key: order){
            if(key.first == OrderByType::ASC){
                if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareLessThan(key.second->Evaluate(&b, schema)))) {
                    return true;
                } else if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareGreaterThan(key.second->Evaluate(&b, schema)))) {
                    return false;
                }
            } else if(key.first == OrderByType::DESC){
                if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareGreaterThan(key.second->Evaluate(&b, schema)))) {
                    return true;
                } else if (static_cast<bool>(key.second->Evaluate(&a, schema).CompareLessThan(key.second->Evaluate(&b, schema)))) {
                    return false;
                }
            }
        }
        return false;
    };
    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);
    bool status = child_executor_->Next(&child_tuple,&child_rid);
    tuples_.clear();
    while(status){
        heap.push(child_tuple);
        if(heap.size() > plan_->GetN()){
            heap.pop();
        }
        status = child_executor_->Next(&child_tuple,&child_rid);
    }
    while(!heap.empty()){
        
    }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
