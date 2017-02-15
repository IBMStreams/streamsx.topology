/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Functionality related to accessing
 * information from the pe/operator context.
 */

#ifndef __SPL__SPLPY_EC_API_H
#define __SPL__SPLPY_EC_API_H

#include <sstream>

#if (PY_MAJOR_VERSION == 3) && \
     ((_IBM_STREAMS_VER_ > 4) || \
      ((_IBM_STREAMS_VER_ == 4) && (_IBM_STREAMS_REL_ >= 2)))

#define __SPLPY_EC_MODULE_OK 1
#define __SPLPY_EC_MODULE_NAME "_streamsx_ec"

#define __SPLPY_EC_MODULE_NAME_LIT "_streamsx_ec.op_"

namespace streamsx {
  namespace topology {

    /**
     * Create the capsule name for an operator.
     * The returned pointer must be freed when it
     * is no longer needed, note that PyCapsule_New
     * requires a char * that outlives it.
     */
    inline const char * _opCaptureName(SPL::Operator *op) {
        std::stringstream cn;
        cn << __SPLPY_EC_MODULE_NAME_LIT << op->getIndex();
        return strdup(cn.str().c_str());
    }
}}

#else
#define __SPLPY_EC_MODULE_OK 0
#endif
#endif

