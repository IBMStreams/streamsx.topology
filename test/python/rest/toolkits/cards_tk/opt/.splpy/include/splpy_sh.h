#ifndef SPL_SPLPY_SH_H_
#define SPL_SPLPY_SH_H_

#include <SPL/Runtime/Operator/State/ConsistentRegionContext.h>
#include <SPL/Runtime/Operator/State/StateHandler.h>
#include <SPL/Runtime/Utility/Mutex.h>

// Support for consistent region and checkpointing in python operators

inline void MY_OPERATOR::checkpoint(SPL::Checkpoint & ckpt) {
  SPL::AutoMutex am(mutex_);
  SPLAPPTRC(L_TRACE, "checkpoint: enter", "python");
  op()->checkpoint(ckpt);
  SPLAPPTRC(L_TRACE, "checkpoint: exit", "python");
}

inline void MY_OPERATOR::reset(SPL::Checkpoint & ckpt) {
  SPL::AutoMutex am(mutex_);
  SPLAPPTRC(L_TRACE, "reset: enter", "python");
  op()->reset(ckpt);
  SPLAPPTRC(L_TRACE, "reset: exit", "python");
}

inline void MY_OPERATOR::resetToInitialState() {
  SPL::AutoMutex am(mutex_);
  SPLAPPTRC(L_TRACE, "resetToInitialState: enter", "python");
  op()->resetToInitialState();
  SPLAPPTRC(L_TRACE, "resetToInitialState: exit", "python");
}
#endif
