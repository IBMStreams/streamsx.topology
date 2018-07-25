#ifndef SPL_SPLPY_CR_H_
#define SPL_SPLPY_CR_H_

#include <SPL/Runtime/Operator/State/ConsistentRegionContext.h>
#include <SPL/Runtime/Utility/Mutex.h>

// Support for consistent region in python operators

template<bool b>
class OptionalConsistentRegionContext {
};

template<>
class OptionalConsistentRegionContext<false> {
 public:
  OptionalConsistentRegionContext(SPL::Operator * op) {}
  class Permit {
  public:
    Permit(OptionalConsistentRegionContext<false>){}
  };
};

template<>
class OptionalConsistentRegionContext<true> {
 public:
  OptionalConsistentRegionContext(SPL::Operator *op) : crContext(NULL) {
    crContext = static_cast<SPL::ConsistentRegionContext *>(op->getContext().getOptionalContext(CONSISTENT_REGION));
  }
  operator SPL::ConsistentRegionContext *() {return crContext;}
  typedef SPL::ConsistentRegionPermit Permit;

private:
  SPL::ConsistentRegionContext * crContext;
};

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



#endif // SPL_SPLPY_CR_H_
