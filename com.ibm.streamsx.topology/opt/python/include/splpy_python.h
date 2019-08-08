/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Consistent inclusion of Python.h
 *
 */

#ifndef __SPL__SPLPY_PYTHON_H
#define __SPL__SPLPY_PYTHON_H

#if PY_MAJOR_VERSION == 3
#define Py_LIMITED_API ((PY_MAJOR_VERSION << 24) | (PY_MINOR_VERSION << 16))
#endif

#include "Python.h"

#endif

