/*
** Copyright (C) 2017  International Business Machines Corporation
** All Rights Reserved
*/

#ifndef PARSEDATESTRING_H_
#define PARSEDATESTRING_H_

#include <curl/curl.h>

#include "SPL/Runtime/Function/SPLFunctions.h"

#define ESC_CHAR '\\' 
namespace com { namespace ibm { namespace streamsx { namespace inet {

        static SPL::int64 parseDatestring(SPL::rstring datestring) { return curl_getdate(datestring.c_str(), NULL); }

} } } }

#endif /* PARSEDATESTRING_H_ */
