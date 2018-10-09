#ifndef SPL_SPLPY_CR_H_
#define SPL_SPLPY_CR_H_

#include <SPL/Runtime/Operator/State/ConsistentRegionContext.h>
#include <SPL/Runtime/Operator/State/StateHandler.h>
#include <SPL/Runtime/Utility/Mutex.h>

#include "splpy_general.h"

// Support for consistent region and checkpointing in python operators

namespace streamsx {
  namespace topology {

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
 * checkpointExtra, resetExtra, and resetToInitialStateExtra, but if operators 
 * have state (such as a Window) that needs to be checkpointed and reset, 
 * they can override the methods.  The lock will be held when these methods 
 * are called.
 */
class DelegatingStateHandler : public SPL::StateHandler {
public:
  DelegatingStateHandler() {}
  virtual ~DelegatingStateHandler() {}

  virtual void drain() {}
  virtual void checkpoint(SPL::Checkpoint & ckpt);
  virtual void reset(SPL::Checkpoint & ckpt);
  virtual void resetToInitialState();

protected:
  /**
   * Save extra state during checkpointing.  The base class will
   * hold the lock when this is called.
   */
  virtual void checkpointExtra(SPL::Checkpoint & ckpt) {}
  /**
   * Reset extra state when resetting to a checkpoint.  The base class will
   * hold the lock when this is called.
   */
  virtual void resetExtra(SPL::Checkpoint & ckpt) {}
  /**
   * Reset extra state when resetting to initial state.  The base class will
   * hold the lock when this is called.
   */
  virtual void resetToInitialStateExtra() {} 

  virtual SplpyOp * op() = 0;

private:
  friend class OptionalAutoLockImpl<true>;

  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }

  SPL::Mutex mutex_;
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
  OptionalAutoLockImpl(void* outer) {}
};

inline void DelegatingStateHandler::checkpoint(SPL::Checkpoint & ckpt) {
  SPLAPPTRC(L_TRACE, "checkpoint: enter", "python");
  OptionalAutoLockImpl<true> lock(this);
  op()->checkpoint(ckpt);
  checkpointExtra(ckpt);
  SPLAPPTRC(L_TRACE, "checkpoint: exit", "python");
}

inline void DelegatingStateHandler::reset(SPL::Checkpoint & ckpt) {
  SPLAPPTRC(L_TRACE, "reset: enter", "python");
  OptionalAutoLockImpl<true> lock(this);
  op()->reset(ckpt);
  resetExtra(ckpt);
  SPLAPPTRC(L_TRACE, "reset: exit", "python");
}

inline void DelegatingStateHandler::resetToInitialState() {
  SPLAPPTRC(L_TRACE, "resetToInitialState: enter", "python");
  OptionalAutoLockImpl<true> lock(this);
  op()->resetToInitialState();
  resetToInitialStateExtra();
  SPLAPPTRC(L_TRACE, "resetToInitialState: exit", "python");
}

}} // end namspace


namespace SPL {

  // Template specializations to support checkpoint/reset of PyObject *
  template<typename T> struct WTDereferencer;
  template<>
  struct WTDereferencer<PyObject *>
  {
    // We don't allow PyObject * to be dereferenced, so the dereference
    // type is PyObject *.
    typedef PyObject * deref_type;
    static PyObject * deref(PyObject *t){ return t; }
    static PyObject const * deref(PyObject const * t) { return t; }
  };

  template<typename T> struct WTReferencer;
  template<>
  struct WTReferencer <PyObject *>
  {
    typedef PyObject * ref_type;
    static PyObject * ref(PyObject * t) { return t; }
    static PyObject const * ref(PyObject const * t) { return t; }
  };

  template<typename T> struct Allocator;
  template<>
  struct Allocator<PyObject *>
  {
    // allocate: seems to be no good way.   It seems not to be needed anyway.

    static void deallocate(PyObject * t) {
      // To deallocate PyObject * , DECREF it rather than deleting it.
      streamsx::topology::SplpyGIL gil;
      Py_XDECREF(t);
    }
  };

  template<typename T> struct Referencer;
  template<>
  struct Referencer<PyObject *>
  {
    // We don't allow PyObject * to be dereferenced, so the dereference
    // type is PyObject *.
    typedef PyObject * dereference_t;
    typedef PyObject * reference_t;
    static PyObject * dereference(PyObject * t) { return t; }
    static PyObject const * dereference(PyObject const * t) { return t; }
    static PyObject * reference(PyObject * t) { return t; }
    static PyObject const * reference(PyObject const * t) { return t; }
  };

  ByteBuffer<Checkpoint> & operator<<(ByteBuffer<Checkpoint> & ckpt, PyObject * obj){
    using namespace streamsx::topology;

    SPLAPPTRC(L_TRACE, "operator << (ByteBuffer<Checkpoint> &, PyObject *): enter", "python");

    SPL::blob bytes;
    {
      SplpyGIL gil;

      // TODO ideally we don't load dumps every time.
      PyObject * dumps = SplpyGeneral::loadFunction("dill", "dumps");
      PyObject * args = PyTuple_New(1);
      Py_INCREF(obj);
      PyTuple_SET_ITEM(args, 0, obj);
      PyObject * ret = PyObject_CallObject(dumps, args);
      if (ret == NULL) {
        SplpyGeneral::tracePythonError();
        Py_DECREF(dumps);
        Py_DECREF(args);
        throw SplpyGeneral::pythonException("dill.dumps");
      }
      Py_DECREF(dumps);
      Py_DECREF(args);
      // ret is the dilled object
      pySplValueFromPyObject(bytes, ret);
      Py_DECREF(ret);
    }
    ckpt << bytes;

    SPLAPPTRC(L_TRACE, "operator << (ByteBuffer<Checkpoint>&, PyObject *): exit", "python");
    return ckpt;
  }

  ByteBuffer<Checkpoint> & operator>>(ByteBuffer<Checkpoint> & ckpt, PyObject * &obj){
    using namespace streamsx::topology;
    SPLAPPTRC(L_TRACE, "operator >> (ByteBuffer<Checkpoint>&, PyObject *&): enter", "python");
    SPL::blob bytes;
    ckpt >> bytes;

    SplpyGIL gil;

    // TODO ideally we don't load loads every time.
    PyObject * loads = SplpyGeneral::loadFunction("dill", "loads");
    PyObject * args = PyTuple_New(1);
    PyObject * pickle = pySplValueToPyObject(bytes);
    PyTuple_SET_ITEM(args, 0, pickle);
    PyObject * ret = PyObject_CallObject(loads, args);
    if (!ret) {
      SplpyGeneral::tracePythonError();
      Py_DECREF(loads);
      Py_DECREF(args);
      throw SplpyGeneral::pythonException("dill.loads");
    }
    Py_DECREF(loads);
    Py_DECREF(args);
    obj = ret;

    SPLAPPTRC(L_TRACE, "operator >> (ByteBuffer<Checkpoint>&, PyObject *&): exit", "python");

    return ckpt;
  }
}

// In the SPL window library, there are some cases in which diagnostic 
// messages with the contents of a window are produced.  For these messages,
// the window library depends on an implementation of operator << for
// the type of object contained in the window.  This method is included
// to support those diagnostic messages.  Because this is not critical,
// errors are intentionally ignored.
std::ostream & operator << (std::ostream &ostr, PyObject * obj){
  using namespace streamsx::topology;
  SplpyGIL gil;
  Py_INCREF(obj);
  PyObject* str = PyObject_Str(obj);
  SPL::rstring s;
  if (str && 0 == pyRStringFromPyObject(s, str)) {
    ostr << s;
  }
  Py_XDECREF(str);
  return ostr;
}


#endif // SPL_SPLPY_CR_H_
