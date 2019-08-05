#ifndef HTTP_FOR_STREAMS
#define HTTP_FOR_STREAMS
#include "curl/curl.h"
#include <SPL/Runtime/Type/Blob.h>
#include <SPL/Runtime/Type/List.h>

namespace com_ibm_streamsx_inet_http {


// We're just writing bytes.
size_t populate_rstring(char *ptr,size_t size, size_t nmemb, void*userdata);

SPL::rstring httpGet(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, SPL::int32 & error);

SPL::rstring httpGet(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, SPL::list<SPL::rstring> & headers, SPL::int32 & error);

SPL::rstring httpGet(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpGet(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, SPL::list<SPL::rstring> & headers, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpGet(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, 
		const SPL::rstring & username, const SPL::rstring & password, 
		const SPL::rstring & certFile, const SPL::rstring &certType, 
		const SPL::rstring & keyFile, const SPL::rstring &keyType, const SPL::rstring &keyPass,
		SPL::list<SPL::rstring> & headers, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpDelete(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, SPL::int32 & error);

SPL::rstring httpDelete(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpDelete(const SPL::rstring & url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring & username, const SPL::rstring & password, 
		const SPL::rstring & certFile, const SPL::rstring &certType, 
		const SPL::rstring & keyFile, const SPL::rstring &keyType, const SPL::rstring &keyPass,
		SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpPut(const SPL::rstring &  data, const  SPL::rstring &  url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring &  username, const SPL::rstring & password, SPL::list<SPL::rstring>& headers, SPL::int32 & error);

SPL::rstring httpPut(const SPL::rstring &  data, const  SPL::rstring &  url, const SPL::list<SPL::rstring> & extraHeaders, const SPL::rstring &  username, const SPL::rstring & password, SPL::list<SPL::rstring>& headers, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpPut(const SPL::rstring &  data, const  SPL::rstring &  url, const SPL::list<SPL::rstring> & extraHeaders, 
		const SPL::rstring &  username, const SPL::rstring & password, 
		const SPL::rstring & certFile, const SPL::rstring &certType, 
		const SPL::rstring & keyFile, const SPL::rstring &keyType, const SPL::rstring &keyPass,
		SPL::list<SPL::rstring>& headers, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpPost(const SPL::rstring &  data, const  SPL::rstring &  url, const SPL::list<SPL::rstring> & extraHeaders,  const SPL::rstring &  username, const SPL::rstring & password, SPL::list<SPL::rstring>& headers, SPL::int32 & error);

SPL::rstring httpPost(const SPL::rstring &  data, const  SPL::rstring &  url, const SPL::list<SPL::rstring> & extraHeaders,  const SPL::rstring &  username, const SPL::rstring & password, SPL::list<SPL::rstring>& headers, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring httpPost(const SPL::rstring &  data, const  SPL::rstring &  url, const SPL::list<SPL::rstring> & extraHeaders,  
		const SPL::rstring &  username, const SPL::rstring & password, 
		const SPL::rstring & certFile, const SPL::rstring &certType, 
		const SPL::rstring & keyFile, const SPL::rstring &keyType, const SPL::rstring &keyPass,
		SPL::list<SPL::rstring>& headers, SPL::int32 & error, const SPL::int32 requestTimeout, const SPL::int32 connectionTimeout);

SPL::rstring urlEncode(const SPL::rstring & raw);

SPL::rstring urlDecode(const SPL::rstring & encoded);


}
#endif
