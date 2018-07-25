#ifndef SPL_SPLPY_CR_H_
#define SPL_SPLPY_CR_H_

#include <SPL/Runtime/Operator/State/ConsistentRegionContext.h>
#include <SPL/Runtime/Operator/State/StateHandler.h>
#include <SPL/Runtime/Utility/Mutex.h>

// Support for consistent region in python operators

namespace streamsx {
  namespace topology {

template<bool b>
class OptionalConsistentRegionContextImpl;

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

#if 0
// Template for an optional value, resolved at compile-time.  Instantiate
// with b == true if the value is needed, b == false if not.
template<bool b, typename T>
class OptionalValue;

template<typename T>
class OptionalValue<false,T>{
public:
  OptionalValue<false, T>() {}
  OptionalValue<false, T>(T & t) {}
  operator bool() { return false; }
};

template<typename T>
class OptionalValue<false, T*> {
public:
  OptionalValue<false, T*>() {}
  OptionalValue<false, T*>(T * t) {}
  operator bool() { return false; }
  T* operator ->() { return static_cast<T*>(0); }
};

template<typename T>
class OptionalValue<true, T> {
public:
  OptionalValue<true, T>() : value() {}
  OptionalValue<true, T>(T & t): value(t) {}
  OptionalValue<true, T> & operator = (T & t) {value = t; return *this;}
  operator bool() { return true; }
  operator T & () { return value; }
private:
  T value;
};

template<typename T>
class OptionalValue<true, T*> {
public:
  OptionalValue<true, T*>() : value() {}
  OptionalValue<true, T*>(T * t): value(t) {}
  OptionalValue<true, T*> & operator = (T & t) {value = t; return *this;}
  operator bool() { return (value != NULL); }
  operator T * () { return value; }
  T * operator ->() { return value; }
private:
  T * value;
};

template<bool b>
class OptionalAutoMutexImpl;

template<>
class OptionalAutoMutexImpl<false> {
public:
  OptionalAutoMutexImpl<false>(OptionalValue<false, SPL::Mutex> &){}
};

template<>
class OptionalAutoMutexImpl<true> {
public:
  OptionalAutoMutexImpl<true>(OptionalValue<true, SPL::Mutex> & m) :
    mutex(static_cast<SPL::Mutex &>(m))
  {
    mutex.lock();
  }
  ~OptionalAutoMutexImpl<true>() {
    mutex.unlock();
  }
private:
  SPL::Mutex & mutex;
};
#endif

class DelegatingStateHandler : public SPL::StateHandler {
public:
  DelegatingStateHandler():locked_(false) {}

  virtual void checkpoint(SPL::Checkpoint & ckpt);
  virtual void reset(SPL::Checkpoint & ckpt);
  virtual void resetToInitialState();

  template<bool b>
  friend class OptionalAutoLockImpl;

protected:
  virtual SplpyOp * op() = 0;

private:
  void lock();
  void unlock();

  SPL::Mutex mutex_;
  bool locked_;
};

template<bool b>
class OptionalAutoLockImpl;

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
