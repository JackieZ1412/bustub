//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cassert>
#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  LOG_DEBUG("Lock Table Function");
  auto iso_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();
  std::cout << "The txn id is: " << txn->GetTransactionId() << std::endl;
  std::cout << "The table id is: " << oid << std::endl;
  // detect the illegal states
  if(iso_level == IsolationLevel::READ_COMMITTED) {
    LOG_DEBUG("It's a READ_COMMIT iso level");
    if(txn_state == TransactionState::SHRINKING){
      LOG_DEBUG("It's in shringking state");
      bool correct_lock = (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED);
      if(!correct_lock){
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("LOCK_ON_SHRINKING");
        TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  if(iso_level == IsolationLevel::REPEATABLE_READ){
    if(txn_state == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if(iso_level == IsolationLevel::READ_UNCOMMITTED){
    bool correct_lock = (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE);
    if(!correct_lock){
      txn->SetState(TransactionState::ABORTED);
      TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    } else {
      if(txn_state == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // lock the map and starts setting in legal states
  table_lock_map_latch_.lock();
  if(table_lock_map_.find(oid) == table_lock_map_.end()){
    table_lock_map_.emplace(oid,std::make_shared<LockRequestQueue>());
  }
  // !!! Changed here
  std::shared_ptr<LockRequestQueue> lrq = table_lock_map_.find(oid)->second;
  lrq->latch_.lock();
  table_lock_map_latch_.unlock();
  for(auto req: lrq->request_queue_){
    if(req->txn_id_ == txn->GetTransactionId()){
      // the same transaction and same table
      auto req_lock_mode = req->lock_mode_;
      if(req_lock_mode == lock_mode){
        lrq->latch_.unlock();
        return true;
      } else {

        bool cond = IsHigherLevel(req_lock_mode,lock_mode);
        if(cond){
          // need to upgrade
          if(lrq->upgrading_ != INVALID_TXN_ID){
            txn->SetState(TransactionState::ABORTED);
            lrq->latch_.unlock();
            TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
          } else {
            // TODO:: release the holding locks lower level before
            lrq->request_queue_.remove(req);
            DeleteTableLockFromSet(txn,req);
            // Using upgrade request emplace the low level lock request
            std::shared_ptr<LockRequest> upgrade_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

            auto req_iter = lrq->request_queue_.begin();
            for(req_iter = lrq->request_queue_.begin(); req_iter != lrq->request_queue_.end(); req_iter++){
              if(!((*req_iter)->granted_)){
                break;
              }
            }
            lrq->request_queue_.insert(req_iter,upgrade_req);
            lrq->upgrading_ = txn->GetTransactionId();

            std::unique_lock<std::mutex> lock(lrq->latch_, std::adopt_lock);
            while(!GrantLock(lrq,upgrade_req)){
              lrq->cv_.wait(lock);
              if(txn->GetState() == TransactionState::ABORTED){
                lrq->upgrading_ = INVALID_TXN_ID;
                lrq->request_queue_.remove(upgrade_req);
                lrq->cv_.notify_all();
                return false;
              }
            }

            lrq->upgrading_ = INVALID_TXN_ID;
            upgrade_req->granted_ = true;
            InsertTableLockFromSet(txn,upgrade_req);

            if(lock_mode != LockMode::EXCLUSIVE){
              lrq->cv_.notify_all();
            }
            return true;

          }
        } else {
          bool c = IsHigherLevel(lock_mode,req_lock_mode);
          if(c){
            // Under these circumstances, the new request lock mode is under some of the request in the same txn, then it is no need to upgrade the request
            // I think it may be true
            lrq->latch_.unlock();
            return true;
          }
          txn->SetState(TransactionState::ABORTED);
          lrq->latch_.unlock();
          TransactionAbortException(txn->GetTransactionId(),AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
    }
  }
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lrq->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lrq->latch_, std::adopt_lock);
  while(!GrantLock(lrq,lock_request)){
    lrq->cv_.wait(lock);
    if(txn->GetState() == TransactionState::ABORTED){
      lrq->upgrading_ = INVALID_TXN_ID;
      lrq->request_queue_.remove(lock_request);
      lrq->cv_.notify_all();
      return false;
    }
  }

  // lrq->upgrading_ = INVALID_TXN_ID;
  lock_request->granted_ = true;
  InsertTableLockFromSet(txn,lock_request);

  if(lock_mode != LockMode::EXCLUSIVE){
    lrq->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  table_lock_map_latch_.lock();
  if(table_lock_map_.find(oid) == table_lock_map_.end()){
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto srs = txn->GetSharedRowLockSet();
  auto xrs = txn->GetExclusiveRowLockSet();

  if(!((srs->find(oid) == srs->end() || (*srs)[oid].empty()) && (xrs->find(oid) == xrs->end() || (*xrs)[oid].empty()))){
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    TransactionAbortException(txn->GetTransactionId(),AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  std::shared_ptr<LockRequestQueue> lrq = table_lock_map_[oid];
  lrq->latch_.lock();
  table_lock_map_latch_.unlock();
  // bool exist = false;
  auto iso_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();
  for(auto &lr: lrq->request_queue_){
    if(!lr->granted_){
      continue;
    }
    if(lr->txn_id_ == txn->GetTransactionId()){
      if(iso_level == IsolationLevel::REPEATABLE_READ){
        if(lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE){
          if(!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      if(iso_level == IsolationLevel::READ_COMMITTED){
        if(lr->lock_mode_ == LockMode::EXCLUSIVE){
          if(!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      if(iso_level == IsolationLevel::READ_UNCOMMITTED){
        if(lr->lock_mode_ == LockMode::EXCLUSIVE){
          if(!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      lrq->request_queue_.remove(lr);
      lrq->cv_.notify_all();
      lrq->latch_.unlock();

      DeleteTableLockFromSet(txn,lr);
      return true;
    }
  }
  lrq->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    txn->SetState(TransactionState::ABORTED);
    TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  auto iso_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();
  // detect the illegal states
  if(iso_level == IsolationLevel::READ_COMMITTED) {
    if(txn_state == TransactionState::SHRINKING){
      bool correct_lock = (lock_mode == LockMode::SHARED);
      if(!correct_lock){
        txn->SetState(TransactionState::ABORTED);
        TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  if(iso_level == IsolationLevel::REPEATABLE_READ){
    if(txn_state == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if(iso_level == IsolationLevel::READ_UNCOMMITTED){
    bool correct_lock = (lock_mode == LockMode::EXCLUSIVE);
    if(!correct_lock){
      txn->SetState(TransactionState::ABORTED);
      TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    } else {
      if(txn_state != TransactionState::GROWING){
        txn->SetState(TransactionState::ABORTED);
        TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }

  // two more checks: must get table locks before row locks
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  // !!! Didn't figure out whether it's necessary to check on shared lock
  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  // lock the map and starts setting in legal states
  row_lock_map_latch_.lock();
  if(row_lock_map_.find(rid) == row_lock_map_.end()){
    row_lock_map_.emplace(rid,std::make_shared<LockRequestQueue>());
  }
  std::shared_ptr<LockRequestQueue> lrq = row_lock_map_[rid];
  lrq->latch_.lock();
  row_lock_map_latch_.unlock();
  for(auto &req: lrq->request_queue_){
    if(req->rid_ == rid && req->txn_id_ == txn->GetTransactionId()){
      // the same transaction and same table
      auto req_lock_mode = req->lock_mode_;
      if(req_lock_mode == lock_mode){
        lrq->latch_.unlock();
        return true;
      } else {

        bool cond = IsHigherLevel(req_lock_mode,lock_mode);
        if(cond){
          // need to upgrade
          if(lrq->upgrading_ != INVALID_TXN_ID){
            txn->SetState(TransactionState::ABORTED);
            lrq->latch_.unlock();
            TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
          } else {
            // TODO:: release the holding locks lower level before
            lrq->request_queue_.remove(req);
            DeleteRowLockFromSet(txn,req);
            // Using upgrade request emplace the low level lock request
            std::shared_ptr<LockRequest> upgrade_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

            auto req_iter = lrq->request_queue_.begin();
            for(req_iter = lrq->request_queue_.begin(); req_iter != lrq->request_queue_.end(); req_iter++){
              if(!((*req_iter)->granted_)){
                break;
              }
            }
            lrq->request_queue_.insert(req_iter,upgrade_req);
            lrq->upgrading_ = txn->GetTransactionId();

            std::unique_lock<std::mutex> lock(lrq->latch_, std::adopt_lock);
            while(!GrantLock(lrq,upgrade_req)){
              lrq->cv_.wait(lock);
              if(txn->GetState() == TransactionState::ABORTED){
                lrq->upgrading_ = INVALID_TXN_ID;
                lrq->request_queue_.remove(upgrade_req);
                lrq->cv_.notify_all();
                return false;
              }
            }

            lrq->upgrading_ = INVALID_TXN_ID;
            upgrade_req->granted_ = true;
            InsertRowLockFromSet(txn,upgrade_req);

            if(lock_mode != LockMode::EXCLUSIVE){
              lrq->cv_.notify_all();
            }
            return true;

          }
        } else {
          bool c = IsHigherLevel(lock_mode,req_lock_mode);
          if(c){
            // Under these circumstances, the new request lock mode is under some of the request in the same txn, then it is no need to upgrade the request
            // I think it may be true
            lrq->latch_.unlock();
            return true;
          }
          txn->SetState(TransactionState::ABORTED);
          lrq->latch_.unlock();
          TransactionAbortException(txn->GetTransactionId(),AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
    }
  }
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lrq->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lrq->latch_, std::adopt_lock);
  while(!GrantLock(lrq,lock_request)){
    lrq->cv_.wait(lock);
    if(txn->GetState() == TransactionState::ABORTED){
      lrq->upgrading_ = INVALID_TXN_ID;
      lrq->request_queue_.remove(lock_request);
      lrq->cv_.notify_all();
      return false;
    }
  }

  // lrq->upgrading_ = INVALID_TXN_ID;
  lock_request->granted_ = true;
  InsertRowLockFromSet(txn,lock_request);

  if(lock_mode != LockMode::EXCLUSIVE){
    lrq->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  row_lock_map_latch_.lock();
  if(row_lock_map_.find(rid) == row_lock_map_.end()){
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> lrq = row_lock_map_[rid];
  lrq->latch_.lock();
  row_lock_map_latch_.unlock();
  // bool exist = false;
  auto iso_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();
  for(auto &lr: lrq->request_queue_){
    if(!lr->granted_){
      continue;
    }
    if(lr->txn_id_ == txn->GetTransactionId()){
      if(iso_level == IsolationLevel::REPEATABLE_READ){
        if(lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE){
          if(!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      if(iso_level == IsolationLevel::READ_COMMITTED){
        if(lr->lock_mode_ == LockMode::EXCLUSIVE){
          if(!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      if(iso_level == IsolationLevel::READ_UNCOMMITTED){
        if(lr->lock_mode_ == LockMode::EXCLUSIVE){
          if(!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }

      DeleteRowLockFromSet(txn,lr);
      return true;
    }
  }
  lrq->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  std::unordered_set<txn_id_t> visited;
  std::stack<txn_id_t> txn_stack;
  for(auto &start_txn: txn_set_){
    if(visited.find(start_txn) != visited.end()){
      // get a cycle
      *txn_id = *visited.begin();
      for (auto const &active_txn_id : visited) {
        *txn_id = std::max(*txn_id, active_txn_id);
      }
      return true;
    }
    txn_stack.push(start_txn);
    while(!txn_stack.empty()){
      auto temp = txn_stack.top();
      txn_stack.pop();
      visited.insert(temp);
      for(auto &next: waits_for_[temp]){
        if(visited.find(next) == visited.end()){
          txn_stack.push(next);
        } else {
          *txn_id = *visited.begin();
          for (auto const &active_txn_id : visited) {
            *txn_id = std::max(*txn_id, active_txn_id);
          }
          return true;
        }
      }
    }
    visited.clear();
  }
  return false;

}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for(auto &edge: waits_for_){
    auto txn1 = edge.first;
    for(auto &txn: edge.second){
      edges.emplace_back(txn1,txn);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}


auto LockManager::IsHigherLevel(LockMode req_lock_mode, LockMode lock_mode) -> bool {
  bool cond1 = (req_lock_mode == LockMode::INTENTION_SHARED && lock_mode != LockMode::INTENTION_SHARED);
  bool cond2 = (req_lock_mode == LockMode::SHARED && (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE));
  bool cond3 = (req_lock_mode == LockMode::INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE));
  bool cond4 = (req_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE);
  return cond1 || cond2 || cond3 || cond4;
}

void LockManager::DeleteTableLockFromSet(Transaction *txn,std::shared_ptr<LockRequest> req){
  LockMode lm = req->lock_mode_;
  if(lm == LockMode::SHARED){
    auto set = txn->GetSharedTableLockSet();
    set->erase(req->oid_);
  } else if(lm == LockMode::INTENTION_SHARED){
    auto set = txn->GetIntentionSharedTableLockSet();
    set->erase(req->oid_);
  } else if(lm == LockMode::EXCLUSIVE){
    auto set = txn->GetExclusiveTableLockSet();
    set->erase(req->oid_);
  } else if(lm == LockMode::INTENTION_EXCLUSIVE){
    auto set = txn->GetIntentionExclusiveTableLockSet();
    set->erase(req->oid_);
  } else if(lm == LockMode::SHARED_INTENTION_EXCLUSIVE){
    auto set = txn->GetSharedIntentionExclusiveTableLockSet();
    set->erase(req->oid_);
  }
}

void LockManager::InsertTableLockFromSet(Transaction *txn,std::shared_ptr<LockRequest> req){
  LockMode lm = req->lock_mode_;
  if(lm == LockMode::SHARED){
    auto set = txn->GetSharedTableLockSet();
    set->insert(req->oid_);
  } else if(lm == LockMode::INTENTION_SHARED){
    auto set = txn->GetIntentionSharedTableLockSet();
    set->insert(req->oid_);
  } else if(lm == LockMode::EXCLUSIVE){
    auto set = txn->GetExclusiveTableLockSet();
    set->insert(req->oid_);
  } else if(lm == LockMode::INTENTION_EXCLUSIVE){
    auto set = txn->GetIntentionExclusiveTableLockSet();
    set->insert(req->oid_);
  } else if(lm == LockMode::SHARED_INTENTION_EXCLUSIVE){
    auto set = txn->GetSharedIntentionExclusiveTableLockSet();
    set->insert(req->oid_);
  }
}

void LockManager::DeleteRowLockFromSet(Transaction *txn,std::shared_ptr<LockRequest> req){
  LockMode lm = req->lock_mode_;
  if(lm == LockMode::SHARED){
    auto map = txn->GetSharedRowLockSet();
    (*map)[req->oid_].erase(req->rid_);
  } else if(lm == LockMode::EXCLUSIVE){
    auto map = txn->GetExclusiveRowLockSet();
    (*map)[req->oid_].erase(req->rid_);
  }
}

void LockManager::InsertRowLockFromSet(Transaction *txn,std::shared_ptr<LockRequest> req){
  LockMode lm = req->lock_mode_;
  if(lm == LockMode::SHARED){
    auto map = txn->GetSharedRowLockSet();
    (*map)[req->oid_].insert(req->rid_);
  } else if(lm == LockMode::EXCLUSIVE){
    auto map = txn->GetExclusiveRowLockSet();
    (*map)[req->oid_].insert(req->rid_);
  }
}

auto LockManager::GrantLock(std::shared_ptr<LockRequestQueue> lrq, std::shared_ptr<LockRequest> want) -> bool {
  for(auto &hold: lrq->request_queue_){
    if(hold->granted_){
      // follow rules by compatibility matrix in P50 of lec16-slides
      if(want->lock_mode_ == LockMode::EXCLUSIVE){
        return false;
      } else if(want->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
        if(hold->lock_mode_ == LockMode::INTENTION_SHARED){
          continue;
        }
        return false;
      } else if(want->lock_mode_ == LockMode::SHARED){
        if(hold->lock_mode_ == LockMode::SHARED || hold->lock_mode_ == LockMode::INTENTION_SHARED){
          continue;
        }
        return false;
      } else if(want->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
        if(hold->lock_mode_ == LockMode::INTENTION_SHARED || hold->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
          continue;
        }
        return false;
      } else if(want->lock_mode_ == LockMode::INTENTION_SHARED){
        if(hold->lock_mode_ == LockMode::EXCLUSIVE){
          return false;
        }
        continue;
      }
    }
  }
  return true;
}


}  // namespace bustub
