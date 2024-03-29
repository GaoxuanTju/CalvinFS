// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "components/scheduler/locking_scheduler.h"

#include <glog/logging.h>
#include <set>
#include <string>
#include "common/source.h"
#include "common/types.h"
#include "common/utils.h"
#include "components/scheduler/lock_manager.h"
#include "components/scheduler/scheduler.h"
#include "components/store/store_app.h"
#include "proto/header.pb.h"
#include "proto/action.pb.h"

using std::set;
using std::string;

REGISTER_APP(LockingScheduler) {
  return new LockingScheduler();
}

void LockingScheduler::MainLoopBody() {
  Action* action;

  // Start processing the next incoming action request.
  if (static_cast<int>(active_actions_.size()) < kMaxActiveActions &&
      running_action_count_ < kMaxRunningActions &&
      action_requests_->Get(&action)) {//gaoxuan --在这一步调用了ActionSource里面的Get函数，这样将这一个请求读取到了action里面
    active_actions_.insert(action->version());
    int ungranted_requests = 0;

    //gaoxuan --下一步就是对请求的读写集进行加锁
    // Request write locks. Track requests so we can check that we don't
    // re-request any as read locks.
    set<string> writeset;
    for (int i = 0; i < action->writeset_size(); i++) {
      if (store_->IsLocal(action->writeset(i))) {
        writeset.insert(action->writeset(i));
        if (!lm_.WriteLock(action, action->writeset(i))) {
          ungranted_requests++;
        }
      }
    }

    // Request read locks.
    for (int i = 0; i < action->readset_size(); i++) {
      // Avoid re-requesting shared locks if an exclusive lock is already
      // requested.
      if (store_->IsLocal(action->readset(i))) {
        if (writeset.count(action->readset(i)) == 0) {
          if (!lm_.ReadLock(action, action->readset(i))) {
            ungranted_requests++;
          }
        }
      }
    }

    // If all read and write locks were immediately acquired, this action
    // is ready to run.
    if (ungranted_requests == 0) {
      running_action_count_++;
      store_->RunAsync(action, &completed_);
    }
  }

  // Process all actions that have finished running.
  while (completed_.Pop(&action)) {
    // Release read locks.
    for (int i = 0; i < action->readset_size(); i++) {
      if (store_->IsLocal(action->readset(i))) {
        lm_.Release(action, action->readset(i));
      }
    }

    // Release write locks. (Okay to release a lock twice.)
    for (int i = 0; i < action->writeset_size(); i++) {
      if (store_->IsLocal(action->writeset(i))) {
        lm_.Release(action, action->writeset(i));
      }
    }

    active_actions_.erase(action->version());
    running_action_count_--;

    /**if (rand() % action->involved_machines() == 0) {
      throughput_++;
    }

    if (start_measure_ == false) {
      start_measure_ = true;
      start_time_ = GetTime();
    } **/
  }

  // Start executing all actions that have newly acquired all their locks.
  while (lm_.Ready(&action)) {
    store_->RunAsync(action, &completed_);
  }

  /**double current_time = GetTime();
  if (start_measure_ == true && current_time - start_time_ > 0.2 && throughput_ > 0) {
    LOG(ERROR) << "[" << machine()->machine_id() << "] "<< "Scheduler:  Throughput is: "<<throughput_/(current_time-start_time_);
    throughput_ = 0;
    start_time_ = current_time;
  } **/
}

