// This is a generated header.  Any modifications will be lost.
#ifndef NL_INETRESOURCE_H
#define NL_INETRESOURCE_H

#include <SPL/Runtime/Utility/FormattableMessage.h>

#define INET_CONSISTENT_CHECK(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0200E", "{0} operator cannot be used inside a consistent region.", true, p0))

#define INET_OPORT_TYPE_CHECK_1(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0201E", "Output attribute ''{0}'' must be of type rstring or list<rstring>. It was declared as type {1}.", true, p0, p1))

#define INET_PARAM_CHECK_1(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0202E", "The emitTuplePerFetch and emitTuplePerURI parameters are valid for list-based attributes only. The attribute was declared as type ''{0}''.", true, p0))

#define INET_PARAM_CHECK_2(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0203E", "Values of the emitTuplePerRecordCount parameter greater than one are valid for list-based attributes only. The declared value was ''{0}''.", true, p0))

#define INET_MALFORMED_URI \
   (::SPL::FormattableMessage0("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0204E", "a URI specified in the URIList parameter contains a syntax error. The Processing Element will shut down now.", true))

#define INET_OPORT_TYPE_CHECK_2(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0207E", "Output attribute ''{0}'' must be of type rstring or list<rstring> or blob or xml. It was declared as type {1}.", true, p0, p1))

#define INET_OPORT_TYPE_CHECK_3(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0208E", "Output attribute ''{0}'' must be of type rstring. It was declared as type {1}.", true, p0, p1))

#define INET_PARAM_CHECK_3(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0209E", "The emitTuplePerFetch and emitTuplePerURI parameters are valid only for attributes of type ''list$lt;rstring>''. The attribute was declared as type ''{0}''.", true, p0))

#define INET_PARAM_CHECK_4(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0210E", "Values of the ''emitTuplePerRecordCount'' parameter greater than one are valid only for attributes of type ''list<rstring>''. The declared value was ''{0}''.", true, p0))

#define INET_FTP_READER_PARAM_CHECK_1 \
   (::SPL::FormattableMessage0("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0211E", "The custom output function ''Binary()'' is not available in operation mode ''isDirReader''.", true))

#define INET_FTP_READER_PARAM_CHECK_2 \
   (::SPL::FormattableMessage0("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0212E", "The custom output function ''Binary()'' is not compatible to output functions ''Line()'', ''FileName()'', ''FileSize()'', ''FileDate()'', ''FileUser()'', ''FileGroup()'', ''FileInfo()'' and ''IsFile()''.", true))

#define INET_FTP_READER_PARAM_CHECK_3 \
   (::SPL::FormattableMessage0("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0213E", "The custom output functions: ''FileName()'', ''FileSize()'', ''FileDate()'', ''FileUser()'', ''FileGroup()'', ''FileInfo()'' and ''IsFile()'' can be used only in directory reader function mode.", true))

#define INET_FTP_READER_OUTPUT_PARAM_CHECK_1 \
   (::SPL::FormattableMessage0("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0214E", "The error output port must have one attribute of type rstring.", true))

#define INET_NONZERO_LIBCURL_RC(p0, p1, p2) \
   (::SPL::FormattableMessage3<typeof(p0),typeof(p1),typeof(p2)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0230W", "A Uniform Resource Identifier retrieval from {0} was not successful. The numeric return code from the libcurl agent was {1,number,integer} and the error message text was: {2}. The Processing Element will continue running.", true, p0, p1, p2))

#define INET_NONZERO_LIBCURL_RC_REPEATED(p0, p1, p2, p3) \
   (::SPL::FormattableMessage4<typeof(p0),typeof(p1),typeof(p2),typeof(p3)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0231W", "A Uniform Resource Identifier retrieval from {0} has not been successful for the last {3,number,integer} attempts. The numeric return code from the libcurl agent was {1,number,integer} and the error message text was: {2}. The Processing Element will continue running.", true, p0, p1, p2, p3))

#define INET_DEPRECATED_COF(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0232W", "Deprecated custom output function used: ''{0}''", true, p0))

#define INET_CONNECTION_SUCCESS(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0240I", "Connection successful ''{0}''", true, p0))

#endif  // NL_INETRESOURCE_H
