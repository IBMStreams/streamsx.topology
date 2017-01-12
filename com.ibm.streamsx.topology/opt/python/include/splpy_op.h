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

namespace streamsx {
  namespace topology {

class SplpyOp {
  public:
      SplpyOp(SPL::Operator *op, const char * spl_setup_py) :
          op_(op),
          pydl_(NULL) {
          pydl_ = SplpySetup::loadCPython(spl_setup_py);
      }

      ~SplpyOp() {
          if (pydl_ != NULL)
             (void) dlclose(pydl_);
      }

      SPL::Operator * op() {
         return op_;
      }

      /**
       * Actions for a Python operator on prepareToShutdown
       * Flush any pending output.
      */
      static void prepareToShutdown() {
          SplpyGILLock lock;
          SplpyGeneral::flush_PyErrPyOut();
      }

   private:
      SPL::Operator *op_;
      // Handle to libpythonX.Y.so
      void * pydl_;
};

}}

#endif

