/* Copyright (C) 2013-2014, International Business Machines Corporation */
/* All Rights Reserved */

#ifndef COM_IBM_STREAMSX_INET_FTP_H_
#define COM_IBM_STREAMSX_INET_FTP_H_

// Define SPL types and functions
#include <SPL/Runtime/Type/String.h>
#include <SPL/Runtime/Type/Float.h>
#include <curl/curl.h>
#include <pthread.h>


namespace com { namespace ibm { namespace streamsx { namespace inet { namespace ftp {

/****************************************************************
 * the common FTP libcurl wrapper                               *
 ****************************************************************/
class FTPWrapper{
public:
	class Initializer{
		bool cryptoLockInserted;
		public:
			Initializer();
			~Initializer();
	};
	static Initializer Init;

	enum CloseConnectionMode {never, ever, punct};
	enum TransmissionProtocolLiteral {ftp, ftpSSLAll, ftpSSLControl, ftpSSLTry, ftps, sftp};
	enum CreateMissingDirs { none, create /*, retry - valid from libcurl 7.19.4*/ };
	typedef size_t (* Callback) (void * buffer, size_t size, size_t count, void * data);


	//Construction and Destruction must not be executed in a multithreading environment
	FTPWrapper(	CloseConnectionMode
				closeConnectionMode_,
				TransmissionProtocolLiteral protocol_,
				bool verbose_,
				CreateMissingDirs createMissingDirs_,
				SPL::rstring const & debugAspect_);
	~FTPWrapper();

	void			onPunct(); //window punct
	void			prepareToShutdown();

	SPL::rstring	getProtocolString() const				{ return SPL::rstring(toString(protocol)); }
	SPL::rstring	getUrl() const							{ return url; } //url is valide after perform was called
	SPL::rstring	getHost() const							{ return host; }
	void			setHost(SPL::rstring const & val);
	SPL::rstring	getPath() const							{ return path; }
	void			setPath(SPL::rstring const & val);		//set path add / at begin if missing
	void			setFilename(SPL::rstring const & val);
	void			setUsername(SPL::rstring const & val);
	void			setPassword(SPL::rstring const & val);
	void			setConnectionTimeout(uint32_t val);
	void			setTransferTimeout(uint32_t val);
	SPL::rstring	getError() const						{ return error; };
	uint32_t		getNoTransfers() const					{ return noTransfers; }
	uint32_t		getNoTransferFailures() const			{ return noTransferFailures; }
	uint32_t		getResultCode() const					{ return static_cast<uint32_t>(resultCodeForCOF); }
	//conversions TransmissionProtocolLiteral
	static char const * toString(TransmissionProtocolLiteral protocol);
	static char const * toString(CloseConnectionMode mode);

protected:
	//internal functions
	void initialize();		//throws in case of error
	void deInitialize();
	bool perform();			//returns true if success throws if initialization fails

	const CloseConnectionMode closeConnectionMode;
	const TransmissionProtocolLiteral protocol;
	const bool verbose;
	const CreateMissingDirs createMissingDirs;
	SPL::rstring schema;
	SPL::rstring url;

	SPL::rstring host;		//the hostname
	SPL::rstring path;		//the path part of the url
	SPL::rstring filename;	//the filename part of the url if any
	bool usernameReceived;
	SPL::rstring username;
	bool passwordReceived;
	SPL::rstring password;
	bool urlChange;
	bool credentialChange;
	SPL::rstring userpasswd;	//the concatenation of username and passwd
	long connectionTimeout;		//the timeout for the connection establishment (CURLOPT_CONNECTTIMEOUT)
	long transferTimeout;		//the timeout for the transfer (CURLOPT_TIMEOUT)
	bool connectionTimeoutReceived;
	bool transferTimeoutReceived;

	CURL * curl; //the curl handle
	CURLcode res; //curl result code
	CURLcode resultCodeForCOF; // The error port's Error() custom output function returns the result code. The res variable cannot be used because it is overridden at many places after the operation failed.
	bool shutdown; //shutdown requested

	//statistics
	uint32_t noTransfers;	//the number of transfers
	uint32_t noTransferFailures;	//the number of failed transfers
	//error and debug
	SPL::rstring error;		//the error string is set when a perform fails
	SPL::rstring action;	//the initialization action internal used
	//multithreading support
	static bool asyncDnsSupport;
	static int wrapperCount;
	static pthread_mutex_t * ssllocks;
	static void lockCallback(int mode, int ind, const char *file, int line);
	//static unsigned long threadId();

	const SPL::rstring debugAspect;
	static char const * protocolString[]; //used for conversion of TransmissionProtocolLiteral
	static char const * closeConnectionModeString[]; //used for conversation
};

/****************************************************************
 * the FTP libcurl wrapper for transports                       *
 ****************************************************************/
class FTPTransportWrapper : public FTPWrapper {
public:
	FTPTransportWrapper(	CloseConnectionMode closeConnectionMode_,
							TransmissionProtocolLiteral protocol_,
							bool verbose_,
							CreateMissingDirs createMissingDirs_,
							SPL::rstring const & debugAspect_,
							bool useEPSV_,
							bool useEPRT_,
							/*bool usePRET_, */
							bool skipPASVIp_);

	~FTPTransportWrapper() {}

	void		setUsePORT(SPL::rstring usePORT_)	{ usePORT = usePORT_; }
	//statistics
	uint64_t	getNoBytesTransferred() const		{ return noBytesTransferred; }
	SPL::float64 getTransferSpeed() const			{ return transferSpeed; }

protected:
	void initialize();
	bool perform();
	void addNoBytesTransferredTemp(uint64_t no) { noBytesTransferredTemp += no; }

	const bool useEPSV;
	const bool useEPRT;
	//const bool usePRET;
	SPL::rstring usePORT;
	const bool skipPASVIp;

	//statistics
	uint64_t noBytesTransferred;
	uint64_t noBytesTransferredTemp;
	SPL::float64 transferSpeed;
};

/****************************************************************
 * the FTP libcurl wrapper for read transport                   *
 ****************************************************************/
class FTPReaderWrapper : public FTPTransportWrapper {
public:
	FTPReaderWrapper(	CloseConnectionMode closeConnectionMode_,
						TransmissionProtocolLiteral protocol_,
						bool verbose_,
						CreateMissingDirs createMissingDirs_,
						SPL::rstring const & debugAspect_,
						bool useEPSV_,
						bool use_EPRT_,
						/*bool usePRET_, */
						bool skipPASVIp_,
						void * op_,
						Callback cb_);

	~FTPReaderWrapper() {}

	bool	perform(); //perform operation - calls ancestor perform

private:
	void	initialize();
	//the call back member
	size_t	writeCallback(void * buffer, size_t size, size_t count);

	void * op;	//the operator to call back
	Callback operatorCallback; //the callback function pointer

	//the write callback function for the curl lib calls the operator call back
	static size_t callback(void * buffer, size_t size, size_t count, void * stream);
};

/****************************************************************
 * the FTP libcurl wrapper for put file transport               *
 ****************************************************************/
class FTPPutFileWrapper : public FTPTransportWrapper {
public:
	FTPPutFileWrapper(	CloseConnectionMode closeConnectionMode_,
						TransmissionProtocolLiteral protocol_,
						bool verbose_,
						CreateMissingDirs createMissingDirs_,
						SPL::rstring const & debugAspect_,
						bool useEPSV_,
						bool useEPRT_,
						/*bool usePRET_, */
						bool skipPASVIp_);

	~FTPPutFileWrapper() {}

	void 			setLocalFilename(SPL::rstring const & val)		{ localFilename = val; }
	SPL::rstring	getLocalFilename() const { return localFilename; }
	void 			setRenameTo(SPL::rstring const & val)			{ renameTo = val; }
	uint64_t 		getFileSize() const							{ return (uint64_t) fsize; }

	bool 			perform(); //perform operation - calls ancestor perform

private:
	void initialize();
	//the call back member
	size_t readCallback(void * ptr, size_t size, size_t count);

	//the read callback function for the curl lib calls the operator call back
	static size_t callback(void * ptr, size_t size, size_t count, void * stream);

	SPL::rstring localFilename; //the name of the local file to open
	SPL::rstring renameTo; //the rename to name - not rename operation if empty

	FILE * fd;
	curl_slist* headerList;
	curl_off_t fsize;

};

/****************************************************************
 * the FTP libcurl wrapper for command execution                *
 ****************************************************************/
class FTPCommandWrapper : public FTPWrapper {
public:
	enum CommandLiteral { none, del, rm, rmdir ,mkdir, rename, modificationTime, modtime, pwd };

	FTPCommandWrapper(	CloseConnectionMode
				closeConnectionMode_,
				TransmissionProtocolLiteral protocol_,
				bool verbose_,
				CreateMissingDirs createMissingDirs_,
				SPL::rstring const & debugAspect_);

	~FTPCommandWrapper() {}

	void			setCommand(SPL::rstring const & val);
	const char*		getCommand() const { return commandLiteralString[commandLiteral]; }
	void			clearCommand() { commandLiteral = none; arg1.clear(), arg2.clear(); }
	void			setArg1(SPL::rstring const & val) { arg1 = val; }
	void			setArg2(SPL::rstring const & val) { arg2 = val; }
	SPL::rstring	getArg1() const { return arg1; }
	SPL::rstring	getArg2() const { return arg2; }
	SPL::rstring	getResult() const { return result; }

	bool			perform(); //perform operation - calls ancestor perform

private:
	void			initialize();

	//the callback function for the curl lib
	size_t			headerCallback(void * buffer, size_t size, size_t count);
	//the callback function for the curl lib
	static size_t 	callback(void * buffer, size_t size, size_t count, void * data);

	CommandLiteral commandLiteral;
	SPL::rstring arg1;
	SPL::rstring arg2;
	SPL::rstring result;
	curl_slist* headerList;

	static char const * commandLiteralString[];
};

}}}}} //namespace
#endif /*COM_IBM_STREAMSX_INET_FTP_H_*/
