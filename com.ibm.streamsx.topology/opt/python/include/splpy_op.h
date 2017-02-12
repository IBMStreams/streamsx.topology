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

#include "splpy_general.h"
#include "splpy_ec_api.h"

namespace streamsx {
  namespace topology {

class SplpyOp {
  public:
      SplpyOp(SPL::Operator *op, const char * spl_setup_py) :
          op_(op),
          pydl_(NULL)

#if __SPLPY_EC_MODULE_OK
          ,opcn_(NULL), opc_(NULL)
#endif

      {
          pydl_ = SplpySetup::loadCPython(spl_setup_py);
#if __SPLPY_EC_MODULE_OK
          opcn_ = streamsx::topology::_opCaptureName(op);

          SplpyGIL lock;
          opc_ = PyCapsule_New((void*)op, opcn_, NULL);
          if (opc_ == NULL)
              throw SplpyGeneral::pythonException("capsule");
#endif
      }

      ~SplpyOp() {
#if __SPLPY_EC_MODULE_OK
          if (opc_ != NULL) {
              SplpyGIL lock;
              Py_DECREF(opc_);
          }
          if (opcn_ != NULL)
              free((void *) opcn_);
#endif
          if (pydl_ != NULL)
             (void) dlclose(pydl_);
      }

      SPL::Operator * op() {
         return op_;
      }

#if __SPLPY_EC_MODULE_OK
      // Get the capture with a new ref
      PyObject * opc() {
         Py_INCREF(opc_);
         return opc_;
      }
#endif

      /**
       * Actions for a Python operator on prepareToShutdown
       * Flush any pending output.
      */
      static void prepareToShutdown() {
          SplpyGIL lock;
          SplpyGeneral::flush_PyErrPyOut();
      }

   private:
      SPL::Operator *op_;
      // Handle to libpythonX.Y.so
      void * pydl_;

#if __SPLPY_EC_MODULE_OK
      // Operator capture name, must outlive
      // the capture
      const char *opcn_;
      // PyCapsule of op_
      PyObject *opc_;
#endif
};

}}

#endif

