#ifndef SPL_SPLPY_CR_H__
#define SPL_SPLPY_CR_H__

#include <SPL/Runtime/Operator/State/ConsistentRegionContext.h>

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

#endif // SPL_SPLPY_CR_H__
