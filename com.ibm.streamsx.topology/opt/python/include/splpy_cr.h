#ifndef SPL_SPLPY_CR_H_
#define SPL_SPLPY_CR_H_

#include <SPL/Runtime/Operator/State/ConsistentRegionContext.h>
#include <SPL/Runtime/Operator/State/StateHandler.h>
#include <SPL/Runtime/Utility/Mutex.h>

// Support for consistent region and checkpointing in python operators

namespace streamsx {
  namespace topology {

/** 
 * OptionalConsistentRegionContext<true> holds a pointer to a 
 * ConsistentRegionContext,
 * and ConsistentRegionContext::Permit is an RAII helper
 * for acquiring and releasing a ConsistentRegionPermit.
 * OptionalConsistentRgionContxt<false> does nothing.
 */
template<bool b>
class OptionalConsistentRegionContextImpl;

/**
 * OptionalAutoLockImpl<bool> is intended to be used with an instance of
 * DelegatingStateHandler.  OptionalAutoLockImpl<false> does nothing,
 * while OptionalAutoLockImpl<true> provides an RAII helper for locking
 * and unlocking, using the same mutex as a  DelegatingStateHandler  instance.
 * This can be used to guard the regions of code that modify the state of the 
 * operator that is saved and restored during checkpointing.  
 */
template<bool b>
class OptionalAutoLockImpl;

/**
 * DelegatingStateHandler implements the StateHandler interface in support
 * of checkpointing and consistent region.  The checkpointing and resetting
 * is delegated to the SplpyOp instance owned by the operator extending
 * this class.  For most operators, it is not necessary to override
 * checkpoint, reset, and resetToInitialState, but if operators have state
 * (such as a Window) that needs to be checkpointed and reset, they can
 * override the methods, but they should be sure to call the base method.
 *
 */
class DelegatingStateHandler : public SPL::StateHandler {
public:
  DelegatingStateHandler():locked_(false) {}

  virtual void checkpoint(SPL::Checkpoint & ckpt);
  virtual void reset(SPL::Checkpoint & ckpt);
  virtual void resetToInitialState();

  friend class OptionalAutoLockImpl<true>;

protected:
  virtual SplpyOp * op() = 0;

private:
  void lock();
  void unlock();

  SPL::Mutex mutex_;
  bool locked_;
};

template<>
class OptionalConsistentRegionContextImpl<false> {
 public:
  OptionalConsistentRegionContextImpl(SPL::Operator * op) {}
  class Permit {
  public:
    Permit(OptionalConsistentRegionContextImpl<false>){}
  };
};

template<>
class OptionalConsistentRegionContextImpl<true> {
 public:
  OptionalConsistentRegionContextImpl(SPL::Operator *op) : crContext(NULL) {
    crContext = static_cast<SPL::ConsistentRegionContext *>(op->getContext().getOptionalContext(CONSISTENT_REGION));
  }
  operator SPL::ConsistentRegionContext *() {return crContext;}
  typedef SPL::ConsistentRegionPermit Permit;

private:
  SPL::ConsistentRegionContext * crContext;
};

template<>
class OptionalAutoLockImpl<true> {
public:
  OptionalAutoLockImpl(DelegatingStateHandler* outer) : outer_(outer) {
    outer->lock();
  }
  ~OptionalAutoLockImpl() {
    outer_->unlock();
  }
private:
  DelegatingStateHandler * outer_;
};

template<>
class OptionalAutoLockImpl<false>
{
 public:
  OptionalAutoLockImpl(DelegatingStateHandler*) {}
};

inline void DelegatingStateHandler::checkpoint(SPL::Checkpoint & ckpt) {
  OptionalAutoLockImpl<true> lock(this);
  op()->checkpoint(ckpt);
}

inline void DelegatingStateHandler::reset(SPL::Checkpoint & ckpt) {
  OptionalAutoLockImpl<true> lock(this);
  op()->reset(ckpt);
}

inline void DelegatingStateHandler::resetToInitialState() {
  OptionalAutoLockImpl<true> lock(this);
  op()->resetToInitialState();
}

inline void DelegatingStateHandler::lock() {
  if (!locked_) {
    mutex_.lock();
    locked_ = true;
  }
}

inline void DelegatingStateHandler::unlock() {
  if (locked_) {
    mutex_.unlock();
    locked_ = false;
  }
}


}} // end namspace

#endif // SPL_SPLPY_CR_H_
