/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2016
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Functionality related to operators
 */

#ifndef __SPL__SPLPY_OP_H
#define __SPL__SPLPY_OP_H

#include <SPL/Runtime/Operator/OperatorMetrics.h>
#include <SPL/Runtime/Common/Metric.h>
#include <SPL/Runtime/Operator/OptionalContext.h>
#include <SPL/Runtime/Operator/State/StateHandler.h>

#include "splpy_general.h"
#include "splpy_setup.h"

#define SPLPY_SH_ASPECT "python,state"
#define SPLPY_ASPECT "python"

namespace streamsx {
  namespace topology {

/**
 * An operator that has a python callable object that can
 * saved and restored in checkpoints.  This defines the methods used
 * by SplpyOpStateHandlerImpl to save and restore a python callable in
 * a checkpoint.
 */
class OperatorWithCallable {
 public:
  OperatorWithCallable();
  virtual ~OperatorWithCallable();

  virtual void clearCallable();
  virtual void setCallable(PyObject * callable);
  inline PyObject * callable() const { return callable_; }
  
  void clearOp();
  void setOp(SPL::Operator * op);
  PyObject * opc() const;

 protected:
  PyObject * callable_;  
  PyObject * opc_;
};

/**
 * Support for saving an operator's state to checkpoints, and restoring
 * the state from checkpoints.
 */
 class SplpyOpStateHandlerImpl : public SPL::StateHandler {
 public:
  SplpyOpStateHandlerImpl(OperatorWithCallable * pyop);
  virtual ~SplpyOpStateHandlerImpl();
  virtual void checkpoint(SPL::Checkpoint & ckpt);
  virtual void reset(SPL::Checkpoint & ckpt);
  virtual void resetToInitialState();

 private:

  static PyObject * call(PyObject * callable, PyObject * arg);

  OperatorWithCallable * op;
  PyObject * loads;
  PyObject * dumps;
  PyObject * pickledInitialCallable;
};

class SplpyOp : public OperatorWithCallable {
  public:
      SplpyOp(SPL::Operator *op, const char * spl_setup_py) :
          op_(op),
          pydl_(NULL),
          exc_suppresses(NULL),
          ckpts_(NULL),
          resets_(NULL),
          stateHandler(NULL)
      {
          pydl_ = SplpySetup::loadCPython(spl_setup_py);
          SplpyGIL lock;
          SPL::rstring outDir(op->getPE().getOutputDirectory());
          PyObject * pyOutDir = pySplValueToPyObject(outDir);
          SplpyGeneral::callVoidFunction(
               "streamsx._streams._runtime", "_setup", pyOutDir, NULL);
          setOp(op);
      }

      virtual ~SplpyOp()
      {
        {
          SplpyGIL lock;
          clearCallable();
          clearOp();
          SplpyGeneral::flush_PyErrPyOut();
        }
        if (pydl_ != NULL)
          (void) dlclose(pydl_);

        delete stateHandler;
        stateHandler = NULL;
      }

      SPL::Operator * op() {
         return op_;
      }

      /**
       * perform any common setup for a Python callable.
       * 
       * - If the callable as an __enter__/__exit__ pair
       *   (__enter__ will have already been called) then create
       *   a metric that keeps track of exceptions suppressed
       *   by __exit__
       */
      void setup(bool needStateHandler) {
          if (PyObject_HasAttrString(callable(), "_streamsx_ec_context")) {
              PyObject *hasContext = PyObject_GetAttrString(callable(), "_streamsx_ec_context");
              if (PyObject_IsTrue(hasContext)) {
                  SPL::OperatorMetrics & metrics = op_->getContext().getMetrics();
                  SPL::Metric &cm = metrics.createCustomMetric(
                      "nExceptionsSuppressed",
                      "Number of exceptions suppressed by callable's __exit__ method.",
                      SPL::Metric::Counter);
                  exc_suppresses = &cm;
              }
              Py_DECREF(hasContext);
          }

          if (needStateHandler)
              setupStateHandler();
      }

      void punct() {
         PyObject *pyObjOnPunct = NULL;
         SplpyGIL lock;
         Py_INCREF(callable());
         pyObjOnPunct = SplpyGeneral::callFunction("streamsx.spl.runtime", "_splpy_on_punct", callable(), NULL);
         bool isCallable = (bool) PyCallable_Check(pyObjOnPunct);
         if (isCallable) {
            PyObject *rv = SplpyGeneral::pyCallObject(pyObjOnPunct, NULL);
            Py_DECREF(rv);
            Py_DECREF(pyObjOnPunct);
         }
         else if (NULL != pyObjOnPunct) {
            Py_DECREF(pyObjOnPunct);
         }
      }


      /**
       * Actions for a Python operator on prepareToShutdown
       * Flush any pending output.
       * Note in distributed we do not interact with the callable here
       * as it may still be needed for concurrent tuple
       * processing. The callable is shutdown and its __exit__
       * method called when this object's destructor is called.
      */
      void prepareToShutdown() {
          SplpyGIL lock;
          SplpyGeneral::callVoidFunction(
               "streamsx.ec", "_prepare_shutdown", NULL, NULL);
          SplpyGeneral::flush_PyErrPyOut();

          if (op()->getPE().isStandalone()) {
              clearCallable();
          }
      }

      int exceptionRaised(const SplpyExceptionInfo& exInfo) {
          if (callable()) {
             // callFunction steals the reference to callable_
             Py_INCREF(callable());
             PyObject *ignore = SplpyGeneral::callFunction(
               "streamsx._streams._runtime", "_call_exit", callable(), exInfo.asTuple());
             int ignoreException = PyObject_IsTrue(ignore);
             Py_DECREF(ignore);
             if (ignoreException && exc_suppresses)
                 exc_suppresses->incrementValue();
             return ignoreException;
          }
          return 0;
      }
     
      /**
       * Register a state handler for the operator.  The state handler
       * handles checkpointing and supports consistent regions.  Checkpointing
       * will be enabled for this operator only if checkpoint is enabled for
       * the topology, this operator is stateful.
       */
      void setupStateHandler() {
        assert(!stateHandler);

        if (op()->getContext().isCheckpointingOn())
          SPLAPPTRC(L_DEBUG, "checkpointing enabled", SPLPY_SH_ASPECT);

        bool consistentRegion = NULL != op()->getContext().getOptionalContext(CONSISTENT_REGION);
        if (consistentRegion)
          SPLAPPTRC(L_DEBUG, "consistent region enabled", SPLPY_SH_ASPECT);

        SPLAPPTRC(L_DEBUG, "Creating state handler", SPLPY_SH_ASPECT);

        stateHandler = new SplpyOpStateHandlerImpl(this);

        createStateMetrics(consistentRegion);
      }

      void createStateMetrics(bool consistentRegion) {
           SPL::OperatorMetrics & metrics = op_->getContext().getMetrics();
           ckpts_ = &(metrics.createCustomMetric(
               "nCheckpoints", "Number of checkpoints.", SPL::Metric::Counter));

          if (consistentRegion) {
              resets_ = &(metrics.createCustomMetric(
                  "nResets", "Number of resets.", SPL::Metric::Counter));
          }
      }

      void checkpoint(SPL::Checkpoint & ckpt) {
          SPLAPPTRC(L_DEBUG, "Op-checkpoint:" << (stateHandler != NULL), SPLPY_SH_ASPECT);
        if (stateHandler) {
          ckpts_->incrementValue();
          stateHandler->checkpoint(ckpt);
        }
      }

      void reset(SPL::Checkpoint & ckpt) {
        if (stateHandler) {
          resets_->incrementValue();
          stateHandler->reset(ckpt);
        }
      }

      void resetToInitialState() {
        if (stateHandler) {
          resets_->incrementValue();
          stateHandler->resetToInitialState();
        }
      }

   private:
      SPL::Operator *op_;

      // Handle to libpythonX.Y.so
      void * pydl_;

      // Number of exceptions suppressed by __exit__
      SPL::Metric *exc_suppresses;
      SPL::Metric *ckpts_;
      SPL::Metric *resets_;

      SPL::StateHandler * stateHandler;
};

 // Steals reference to pickledCallable
 SplpyOpStateHandlerImpl::SplpyOpStateHandlerImpl(OperatorWithCallable * pyop) : op(pyop), loads(), dumps(), pickledInitialCallable() {

  // Load pickle.loads and pickle.dumps
  SplpyGIL lock;
  loads = SplpyGeneral::loadFunction("dill", "loads");
  dumps = SplpyGeneral::loadFunction("dill", "dumps");
  
  pickledInitialCallable = call (dumps, op->callable());
  if (!pickledInitialCallable) {
    SplpyGeneral::tracePythonError();
    throw SplpyExceptionInfo::pythonError("dill.dumps").exception();
  }
 }

 SplpyOpStateHandlerImpl::~SplpyOpStateHandlerImpl() {
   SplpyGIL lock;
   Py_CLEAR(loads);
   Py_CLEAR(dumps);
   Py_CLEAR(pickledInitialCallable);
 }

 void SplpyOpStateHandlerImpl::checkpoint(SPL::Checkpoint & ckpt) {
   SPLAPPTRC(L_DEBUG, "checkpoint-callable: enter", SPLPY_SH_ASPECT);
   SPL::blob bytes;
   {
     SplpyGIL lock;
     PyObject * ret = call(dumps, op->callable());
     if (!ret) {
       SplpyGeneral::tracePythonError();
       throw SplpyGeneral::pythonException("dill.dumps");
     }
     pySplValueFromPyObject(bytes, ret);
     Py_DECREF(ret);
   }
   SPLAPPTRC(L_DEBUG, "checkpoint-callable: dilled callable: bytes:" << bytes.getSize(), SPLPY_SH_ASPECT);
   ckpt << bytes;
   SPLAPPTRC(L_DEBUG, "checkpoint-callable: exit", SPLPY_SH_ASPECT);
 }

 void SplpyOpStateHandlerImpl::reset(SPL::Checkpoint & ckpt) {
   SPLAPPTRC(L_DEBUG, "reset-callable: enter", SPLPY_SH_ASPECT);

   // Restore the callable from an spl blob
   SPL::blob bytes;
   ckpt >> bytes;

   SPLAPPTRC(L_DEBUG, "reset-callable: read data: bytes:" << bytes.getSize(), SPLPY_SH_ASPECT);

   SplpyGIL lock;

   // Release the old callable
   op->clearCallable();

   PyObject * pickle = pySplValueToPyObject(bytes);
   PyObject * callable = call(loads, pickle);
   if (!callable) {
       SplpyGeneral::tracePythonError();
       throw SplpyGeneral::pythonException("dill.loads");
   }
   bytes.clear();
 
   // Switch to newly unpickled callable.
   op->setCallable(callable); // reference to ret stolen by op
   SPLAPPTRC(L_DEBUG, "reset-callable: exit", SPLPY_SH_ASPECT);
 }
 void SplpyOpStateHandlerImpl::resetToInitialState() {
   SPLAPPTRC(L_DEBUG, "resetToInitialState-callable: enter", SPLPY_SH_ASPECT);
   SplpyGIL lock;

   // Release the old callable
   op->clearCallable();

   PyObject * initialCallable = call(loads, pickledInitialCallable);
   if (!initialCallable) {
     SplpyGeneral::tracePythonError();
     throw SplpyGeneral::pythonException("dill.loads");
   }

   op->setCallable(initialCallable);
   SPLAPPTRC(L_DEBUG, "resetToInitialState-callable: exit", SPLPY_SH_ASPECT);
 }

 // Call a python callable with a single argument
 // Caller must hold GILState.
 PyObject * SplpyOpStateHandlerImpl::call(PyObject * callable, PyObject * arg) {
   PyObject * args = PyTuple_New(1);
   Py_INCREF(arg);
   PyTuple_SET_ITEM(args, 0, arg);
   PyObject * ret = PyObject_CallObject(callable, args);
   Py_DECREF(args);
   return ret;
 }

 /**
  * Set or clear the callable for this operator. 
  *
  */
 // This constructor may be called before python has been loaded,
 // so it cannot safely use the python API.
 OperatorWithCallable::OperatorWithCallable() : callable_(NULL), opc_(NULL) {}
 // This destructor may be called after python has been unloaded,
 // so it cannot safely use the python API.
 OperatorWithCallable::~OperatorWithCallable() {}

 void OperatorWithCallable::setOp(SPL::Operator * op) {
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::setOp enter", SPLPY_SH_ASPECT);
   SplpyGIL lock;
   opc_ = PyLong_FromVoidPtr(static_cast<void*>(op));
   if (opc_ == NULL) {
     SPLAPPTRC(L_DEBUG, "OperatorWithCallable::setOp", SPLPY_SH_ASPECT);
     SplpyGeneral::tracePythonError();        
     throw SplpyExceptionInfo::pythonError("capsule").exception();
   }
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::setOp exit", SPLPY_SH_ASPECT);
 }
 void OperatorWithCallable::clearOp() {
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::clearOp enter", SPLPY_SH_ASPECT);
   SplpyGIL lock;
   Py_CLEAR(opc_);  
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::clearOp exit", SPLPY_SH_ASPECT);
 }
 // Get the capture with a new ref
 PyObject * OperatorWithCallable::opc() const {
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::opc enter", SPLPY_SH_ASPECT);
   Py_INCREF(opc_);
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::opc exit", SPLPY_SH_ASPECT);
   return opc_;
 }
   
 void OperatorWithCallable::setCallable(PyObject * callable) {
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::setCallable enter", SPLPY_SH_ASPECT);
   Py_CLEAR(callable_);
   callable_ = callable;
   // Enter the context manager for the callable.
   Py_INCREF(callable);
   SplpyGeneral::callVoidFunction(
     "streamsx._streams._runtime", "_call_enter", callable, opc());
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::setCallable exit", SPLPY_SH_ASPECT);
 }
 void OperatorWithCallable::clearCallable() {
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::clearCallable enter", SPLPY_SH_ASPECT);
   if (callable_) {
     PyObject *cleared = callable_;
     callable_ = NULL;
     // Exit the context manager and release it
     // The function call steals the operator's reference
     SplpyGeneral::callVoidFunction(
       "streamsx._streams._runtime", "_call_exit", cleared, NULL);
   }
   SPLAPPTRC(L_DEBUG, "OperatorWithCallable::clearCallable exit", SPLPY_SH_ASPECT);
 } 
}}


#endif

