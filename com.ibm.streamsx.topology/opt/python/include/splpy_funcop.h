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

#ifndef __SPL__SPLPY_FUNCOP_H
#define __SPL__SPLPY_FUNCOP_H

#include "splpy_op.h"

#include <SPL/Runtime/Operator/ParameterValue.h>
#include <SPL/Runtime/Operator/State/StateHandler.h>


namespace streamsx {
  namespace topology {

using UTILS_NAMESPACE_QUALIFIER AutoMutex;
using UTILS_NAMESPACE_QUALIFIER Mutex;

class SplpyFuncOp : public SplpyOp {
  public:

      SplpyFuncOp(SPL::Operator * op, const std::string & wrapfn) :
        SplpyOp(op, "/opt/python/packages/streamsx/topology"),
          stateHandler(NULL), stateHandlerMutex(NULL)
      {
         setSubmissionParameters();
         addAppPythonPackages();
         loadAndWrapCallable(wrapfn);
         setupStateHandler();
      }

      virtual ~SplpyFuncOp() {
        delete stateHandler;
        stateHandler = NULL;
        stateHandlerMutex = NULL; // not owned by this class
      }

      class RealAutoLock {
      public:
        RealAutoLock(SplpyFuncOp * op) : op_(op) {
          assert(op->stateHandlerMutex);
          op->stateHandlerMutex->lock();
        }
        ~RealAutoLock() {
          op_->stateHandlerMutex->unlock();
        }
      private:
        RealAutoLock(RealAutoLock const & other);
        RealAutoLock();

        SplpyFuncOp * op_;
      };

      class NoAutoLock {
      public:
        NoAutoLock(SplpyFuncOp *) {}
      };

      friend class RealAutoLock;

  private:
      int hasParam(const char *name) {
          return op()->getParameterNames().count(name);
      }

      const SPL::rstring & param(const char *name) {
          return op()->getParameterValues(name)[0]->getValue();
      }

      /**
       * Set submission parameters. Note all functional operators
       * share the same submission parameters (they are topology wide)
       * and thus each operator will execute this code. They will
       * all have the same values.
       *
       * Note at this point all the parameters are rstring values,
       * the Python code does any type conversion.
      */
      void setSubmissionParameters() {
          if (hasParam("submissionParamNames")) {
              SplpyGIL lock;

              const SPL::Operator::ParameterValueListType& names = op()->getParameterValues("submissionParamNames");

              const SPL::Operator::ParameterValueListType& values = op()->getParameterValues("submissionParamValues");

              for (int i = 0; i < names.size(); i++) {
                  PyObject *n = pyUnicode_FromUTF8(names[i]->getValue());
                  PyObject *v = pyUnicode_FromUTF8(values[i]->getValue());

                  streamsx::topology::SplpyGeneral::callVoidFunction(
                        "streamsx.ec", "_set_submit_param", n, v);
              }
          }
      }

      /**
       * Load and wrap the callable that will be invoked
       * by the operator.
      */
      void loadAndWrapCallable(const std::string & wrapfn) {
          SplpyGIL lock;

          // pointer to the application function or callable class
          PyObject * appCallable =
             SplpyGeneral::loadFunction(param("pyModule"), param("pyName"));

          // The object to be called is either appCallable for
          // a function passed into the operator
          // or a pickled encoded class instance
          // represented as a string in parameter pyCallable
    
          if (hasParam("pyCallable")) {
             // argument is the serialized callable instance
             PyObject * appClass = appCallable;
             appCallable = pyUnicode_FromUTF8(param("pyCallable").c_str());
             Py_DECREF(appClass);

#if __SPLPY_EC_MODULE_OK
             setopc();
#endif
          }

          PyObject *extraArg = NULL;
          if (op()->getNumberOfOutputPorts() == 1) {
              extraArg = streamsx::topology::Splpy::pyAttributeNames(
               op()->getOutputPortAt(0));
          }

          setCallable(SplpyGeneral::callFunction(
               "streamsx.topology.runtime", wrapfn, appCallable, extraArg));
      }

      /**
       * Register a state handler for the operator.  The state handler
       * handles checkpointing and supports consistent regions.  Checkpointing
       * will be enabled for this operator only if checkpoint is enabled for
       * the topology, this operator is stateful, and it can be saved and 
       * restored using dill.
       */
      void setupStateHandler() {
        // If the value of the pyStateful param is true, create and register
        // a state handler instance.  Otherwise, register a do-nothing
        // state handler.
        SPL::boolean stateful = static_cast<SPL::boolean>(op()->getParameterValues("pyStateful")[0]->getValue());
	bool checkpointing = op()->getContext().isCheckpointingOn();

        if (checkpointing) {
	  PyObject * pickledCallable = NULL;
	  if (stateful) {
	    // Ensure that callable() can be pickled before using it in a state
	    // handler.
	    
	    SplpyGIL lock;
	    PyObject * dumps = SplpyGeneral::loadFunction("dill", "dumps");
	    PyObject * args = PyTuple_New(1);
	    Py_INCREF(callable());
	    PyTuple_SET_ITEM(args, 0, callable());
	    pickledCallable = PyObject_CallObject(dumps, args);
	    Py_DECREF(args);
	    Py_DECREF(dumps);
	    
	    std::stringstream msg;
	    msg << "Checkpointing is not available for the " << op()->getContext().getName() << " operator";
	    
	    if (!pickledCallable) {
	      // The callable cannot be pickled.  Throw an exception that
              // shuts down the operator, unless it is supressed.
	      // If it is suppressed, this operator will continue to run, but
	      // with no checkpointing enabled.)
	      if (PyErr_Occurred()) {
                SplpyExceptionInfo exceptionInfo = SplpyExceptionInfo::pythonError("setup");
                if (exceptionInfo.pyValue_) {
		  SPL::rstring text;
		  // note pyRStringFromPyObject returns zero on success
                  if (pyRStringFromPyObject(text, exceptionInfo.pyValue_) == 0) {
		    msg << " because of python error " << text;
		  }

                  SPLAPPTRC(L_WARN, msg.str(), "python");
                  // Offer the exception to the operator, so the operator
                  // can suppress it.
                  if (exceptionRaised(exceptionInfo) == 0) {
                    throw exceptionInfo.exception();
                  }
                  exceptionInfo.clear();
                  // The exception was suppressed.  Continue with
                  // pickledCallable == NULL, which will cause a do-nothing
                  // state handler to be created.
                  // Effectively, checkpointing will not be enabled for
                  // this operator even though it is stateful.
                  SPLAPPTRC(L_WARN, "Proceeding with no checkpointing for the " << op()->getContext().getName() << " operator", "python");
                }
              }
              else {
                // This is probably unreachable.  PyObject_CallObject
                // returned NULL, but there was no python exception.
		throw SplpyGeneral::generalException("setup", msg.str());
	      }
            }
	  }
	  assert(!stateHandler);
          if (stateful && pickledCallable) {
            stateHandler = new SplPyFuncOpStateHandlerImpl(this, pickledCallable);
            stateHandlerMutex = stateHandler->getMutex();
          }
          else {
            stateHandler = new SplPyFuncOpStateHandler;
            stateHandlerMutex = NULL;
          }
	  SPLAPPTRC(L_DEBUG, "registerStateHandler", "python");
	  op()->getContext().registerStateHandler(*stateHandler);
	}
      }

      /*
       *  Add any packages in the application directory
       *  to the Python path. The application directory
       *  is passed to each invocation of the functional 
       *  operators as the parameter toolkitDir. The value
       *  passed is the toolkit of the invocation of the operator.
       */
      void addAppPythonPackages() {
          SplpyGIL lock;

          PyObject * tkDir =
            streamsx::topology::pyUnicode_FromUTF8(param("toolkitDir"));

          SplpyGeneral::callVoidFunction(
              "streamsx.topology.runtime", "setupOperator", tkDir, NULL);
      }

      // Do-nothing base class.
      class SplPyFuncOpStateHandler : public SPL::StateHandler {
      private:
        virtual Mutex * getMutex() { return NULL; }
        friend class SplpyFuncOp;
      };

      /**
       * Support for saving an operator's state to checkpoints, and restoring
       * the state from checkpoints.
       */
      class SplPyFuncOpStateHandlerImpl: public SplPyFuncOpStateHandler {
      public:
        // Steals reference to pickledCallable
      SplPyFuncOpStateHandlerImpl(SplpyOp * pyop, PyObject * pickledCallable) : op(pyop), loads(), dumps(), pickledInitialCallable(pickledCallable), mutex_() {
          // Load pickle.loads and pickle.dumps
          SplpyGIL lock;
          loads = SplpyGeneral::loadFunction("dill", "loads");
          dumps = SplpyGeneral::loadFunction("dill", "dumps");
        }

        virtual ~SplPyFuncOpStateHandlerImpl() {
          Py_CLEAR(loads);
          Py_CLEAR(dumps);
          Py_CLEAR(pickledInitialCallable);
        }

        virtual void checkpoint(SPL::Checkpoint & ckpt) {
          SPLAPPTRC(L_DEBUG, "checkpoint", "python");
          AutoMutex am(mutex_);
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
        }

        virtual void reset(SPL::Checkpoint & ckpt) {
          SPLAPPTRC(L_DEBUG, "reset", "python");
          AutoMutex am(mutex_);
          // Restore the callable from an spl blob
          SPL::blob bytes;
          ckpt >> bytes;
          SplpyGIL lock;
          PyObject * pickle = pySplValueToPyObject(bytes);
          PyObject * ret = call(loads, pickle);
          if (!ret) {
            SplpyGeneral::tracePythonError();
            throw SplpyGeneral::pythonException("dill.loads");
          }
          // discard the old callable, replace with the newly
          // unpickled one.
          Py_DECREF(op->callable());
          op->setCallable(ret); // reference to ret stolen by op
        }

       virtual void resetToInitialState() {
         AutoMutex am(mutex_);
         SPLAPPTRC(L_DEBUG, "resetToInitialState", "python");
         SplpyGIL lock;
         PyObject * initialCallable = call(loads, pickledInitialCallable);
         if (!initialCallable) {
           SplpyGeneral::tracePythonError();
           throw SplpyGeneral::pythonException("dill.loads");
         }
         Py_DECREF(op->callable());
         op->setCallable(initialCallable);
       }

      private:
        virtual Mutex* getMutex() {
         return &mutex_;
       }

       // Call a python callable with a single argument
        // Caller must hold GILState.
        PyObject * call(PyObject * callable, PyObject * arg) {
          PyObject * args = PyTuple_New(1);
          Py_INCREF(arg);
          PyTuple_SET_ITEM(args, 0, arg);
          PyObject * ret = PyObject_CallObject(callable, args);
          Py_DECREF(args);
          return ret;
        }

        SplpyOp * op;
        PyObject * loads;
        PyObject * dumps;
        PyObject * pickledInitialCallable;
        Mutex mutex_;
      };

      SplPyFuncOpStateHandler * stateHandler;
      Mutex * stateHandlerMutex;
};

}}

#endif

