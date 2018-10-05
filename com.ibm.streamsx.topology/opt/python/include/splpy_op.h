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

namespace streamsx {
  namespace topology {

class SplpyOp;

/**
 * Support for saving an operator's state to checkpoints, and restoring
 * the state from checkpoints.
 */
 class SplpyOpStateHandlerImpl : public SPL::StateHandler {
 public:
  SplpyOpStateHandlerImpl(SplpyOp * pyop, PyObject * pickledCallable);
  virtual ~SplpyOpStateHandlerImpl();
  virtual void checkpoint(SPL::Checkpoint & ckpt);
  virtual void reset(SPL::Checkpoint & ckpt);
  virtual void resetToInitialState();
 private:
  static PyObject * call(PyObject * callable, PyObject * arg);
  SplpyOp * op;
  PyObject * loads;
  PyObject * dumps;
  PyObject * pickledInitialCallable;
};

class SplpyOp {
  public:
      SplpyOp(SPL::Operator *op, const char * spl_setup_py) :
          op_(op),
          callable_(NULL),
          pydl_(NULL),
          exc_suppresses(NULL),
          ckpts_(NULL),
          resets_(NULL),
          opc_(NULL),
          stateHandler(NULL)
      {
          pydl_ = SplpySetup::loadCPython(spl_setup_py);

          SplpyGIL lock;
          SPL::rstring outDir(op->getPE().getOutputDirectory());
          PyObject * pyOutDir = pySplValueToPyObject(outDir);
          SplpyGeneral::callVoidFunction(
               "streamsx._streams._runtime", "_setup", pyOutDir, NULL);
          opc_ = PyLong_FromVoidPtr((void*)op);
          if (opc_ == NULL)
              throw SplpyGeneral::pythonException("capsule");
      }

      virtual ~SplpyOp()
      {
        {
          SplpyGIL lock;

          clearCallable();

          Py_CLEAR(opc_);

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
       * Set or clear the callable for this operator. 
       *
      */
      void setCallable(PyObject * callable) {
          callable_ = callable;
          // Enter the context manager for the callable.
          Py_INCREF(callable);
          SplpyGeneral::callVoidFunction(
             "streamsx._streams._runtime", "_call_enter", callable, opc());
      }
      void clearCallable() {
        if (callable_) {
             // Exit the context manager and release it
             // THe function call steals the operator's reference
             SplpyGeneral::callVoidFunction(
               "streamsx._streams._runtime", "_call_exit", callable_, NULL);
             callable_ = NULL;
        }
      }

      PyObject * callable() {
          return callable_;
      }

      /**
       * perform any common setup for a Python callable.
       * 
       * - If the callable as an __enter__/__exit__ pair
       *   (__enter__ will have already been called) then create
       *   a metric that keeps track of exceptions suppressed
       *   by __exit__
       */
      void setup(bool stateful) {
          if (PyObject_HasAttrString(callable_, "_streamsx_ec_context")) {
              PyObject *hasContext = PyObject_GetAttrString(callable_, "_streamsx_ec_context");
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

          if (stateful)
              setupStateHandler();
      }

      /**
       * Actions for a Python operator on prepareToShutdown
       * Flush any pending output.
       * Note we do not interact with the callable here
       * as it may still be needed for concurrent tuple
       * processing. The callable is shutdown and its __exit__
       * method called when this object's destructor is called.
       * This means the mutex in the operator is not required
       * when calling this function.
      */
      void prepareToShutdown() {
          SplpyGIL lock;
          SplpyGeneral::flush_PyErrPyOut();
      }

      int exceptionRaised(const SplpyExceptionInfo& exInfo) {
          if (callable_) {
             // callFunction steals the reference to callable_
             Py_INCREF(callable_);
             PyObject *ignore = SplpyGeneral::callFunction(
               "streamsx._streams._runtime", "_call_exit", callable_, exInfo.asTuple());
             int ignoreException = PyObject_IsTrue(ignore);
             Py_DECREF(ignore);
             if (ignoreException && exc_suppresses)
                 exc_suppresses->incrementValue();
             return ignoreException;
          }
          return 0;
      }

      // Get the capture with a new ref
      PyObject * opc() {
         Py_INCREF(opc_);
         return opc_;
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
          SPLAPPTRC(L_DEBUG, "checkpointing enabled", "python");
        else
          SPLAPPTRC(L_DEBUG, "checkpointing disabled", "python");

        bool consistentRegion = NULL != op()->getContext().getOptionalContext(CONSISTENT_REGION);
        if (consistentRegion)
          SPLAPPTRC(L_DEBUG, "consistent region enabled", "python");
        else
          SPLAPPTRC(L_DEBUG, "consistent region disabled", "python");

        // Save the initial callable.
        SplpyGIL lock;
        Py_INCREF(callable());
        PyObject *pickledCallable = SplpyGeneral::callFunction("dill", "dumps", callable(), NULL);

        SPLAPPTRC(L_DEBUG, "Creating state handler", "python");
        // pickledCallable reference stolen here.
        stateHandler = new SplpyOpStateHandlerImpl(this, pickledCallable);

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

      // Python object used to process tuples
      PyObject *callable_;

      // Handle to libpythonX.Y.so
      void * pydl_;

      // Number of exceptions suppressed by __exit__
      SPL::Metric *exc_suppresses;
      SPL::Metric *ckpts_;
      SPL::Metric *resets_;

      // PyLong of op_
      PyObject *opc_;

      SPL::StateHandler * stateHandler;
};

 // Steals reference to pickledCallable
 SplpyOpStateHandlerImpl::SplpyOpStateHandlerImpl(SplpyOp * pyop, PyObject * pickledCallable) : op(pyop), loads(), dumps(), pickledInitialCallable(pickledCallable) {
  // Load pickle.loads and pickle.dumps
  SplpyGIL lock;
  loads = SplpyGeneral::loadFunction("dill", "loads");
  dumps = SplpyGeneral::loadFunction("dill", "dumps");
 }

 SplpyOpStateHandlerImpl::~SplpyOpStateHandlerImpl() {
   SplpyGIL lock;
   Py_CLEAR(loads);
   Py_CLEAR(dumps);
   Py_CLEAR(pickledInitialCallable);
 }

 void SplpyOpStateHandlerImpl::checkpoint(SPL::Checkpoint & ckpt) {
   SPLAPPTRC(L_DEBUG, "checkpoint", "python");
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
   ckpt << bytes;
   SPLAPPTRC(L_TRACE, "exit checkpoint", "python");
 }

 void SplpyOpStateHandlerImpl::reset(SPL::Checkpoint & ckpt) {
   SPLAPPTRC(L_DEBUG, "reset", "python");

   SplpyGIL lock;

   // Release the old callable
   op->clearCallable();

   SPL::blob bytes;
   Py_BEGIN_ALLOW_THREADS
   // Restore the callable from  an spl blob
   ckpt >> bytes;
   Py_END_ALLOW_THREADS

   PyObject * pickle = pySplValueToPyObject(bytes);
   PyObject * callable = call(loads, pickle);
   if (!callable) {
       SplpyGeneral::tracePythonError();
       throw SplpyGeneral::pythonException("dill.loads");
   }
   bytes.clear();
 
   // Switch to newly unpickled callable.
   op->setCallable(callable); // reference to ret stolen by op

 }

 void SplpyOpStateHandlerImpl::resetToInitialState() {
   SPLAPPTRC(L_DEBUG, "resetToInitialState", "python");
   SplpyGIL lock;

   // Release the old callable
   op->clearCallable();

   PyObject * initialCallable = call(loads, pickledInitialCallable);
   if (!initialCallable) {
     SplpyGeneral::tracePythonError();
     throw SplpyGeneral::pythonException("dill.loads");
   }

   op->setCallable(initialCallable);
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

}}

#endif

