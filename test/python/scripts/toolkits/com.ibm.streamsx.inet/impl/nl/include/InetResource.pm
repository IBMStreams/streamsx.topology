# This is a generated module.  Any modifications will be lost.
package InetResource;
use strict;
use Cwd qw(abs_path);
use File::Basename;
unshift(@INC, $ENV{STREAMS_INSTALL} . "/system/impl/bin") if ($ENV{STREAMS_INSTALL});
require SPL::Helper;
my $toolkitRoot = dirname(abs_path(__FILE__)) . '/../../..';

sub INET_CONSISTENT_CHECK($)
{
   my $defaultText = <<'::STOP::';
{0} operator cannot be used inside a consistent region.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0200E", \$defaultText, @_);
}


sub INET_OPORT_TYPE_CHECK_1($$)
{
   my $defaultText = <<'::STOP::';
Output attribute ''{0}'' must be of type rstring or list<rstring>. It was declared as type {1}.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0201E", \$defaultText, @_);
}


sub INET_PARAM_CHECK_1($)
{
   my $defaultText = <<'::STOP::';
The emitTuplePerFetch and emitTuplePerURI parameters are valid for list-based attributes only. The attribute was declared as type ''{0}''.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0202E", \$defaultText, @_);
}


sub INET_PARAM_CHECK_2($)
{
   my $defaultText = <<'::STOP::';
Values of the emitTuplePerRecordCount parameter greater than one are valid for list-based attributes only. The declared value was ''{0}''.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0203E", \$defaultText, @_);
}


sub INET_MALFORMED_URI()
{
   my $defaultText = <<'::STOP::';
a URI specified in the URIList parameter contains a syntax error. The Processing Element will shut down now.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0204E", \$defaultText, @_);
}


sub INET_OPORT_TYPE_CHECK_2($$)
{
   my $defaultText = <<'::STOP::';
Output attribute ''{0}'' must be of type rstring or list<rstring> or blob or xml. It was declared as type {1}.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0207E", \$defaultText, @_);
}


sub INET_OPORT_TYPE_CHECK_3($$)
{
   my $defaultText = <<'::STOP::';
Output attribute ''{0}'' must be of type rstring. It was declared as type {1}.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0208E", \$defaultText, @_);
}


sub INET_PARAM_CHECK_3($)
{
   my $defaultText = <<'::STOP::';
The emitTuplePerFetch and emitTuplePerURI parameters are valid only for attributes of type ''list$lt;rstring>''. The attribute was declared as type ''{0}''.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0209E", \$defaultText, @_);
}


sub INET_PARAM_CHECK_4($)
{
   my $defaultText = <<'::STOP::';
Values of the ''emitTuplePerRecordCount'' parameter greater than one are valid only for attributes of type ''list<rstring>''. The declared value was ''{0}''.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0210E", \$defaultText, @_);
}


sub INET_FTP_READER_PARAM_CHECK_1()
{
   my $defaultText = <<'::STOP::';
The custom output function ''Binary()'' is not available in operation mode ''isDirReader''.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0211E", \$defaultText, @_);
}


sub INET_FTP_READER_PARAM_CHECK_2()
{
   my $defaultText = <<'::STOP::';
The custom output function ''Binary()'' is not compatible to output functions ''Line()'', ''FileName()'', ''FileSize()'', ''FileDate()'', ''FileUser()'', ''FileGroup()'', ''FileInfo()'' and ''IsFile()''.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0212E", \$defaultText, @_);
}


sub INET_FTP_READER_PARAM_CHECK_3()
{
   my $defaultText = <<'::STOP::';
The custom output functions: ''FileName()'', ''FileSize()'', ''FileDate()'', ''FileUser()'', ''FileGroup()'', ''FileInfo()'' and ''IsFile()'' can be used only in directory reader function mode.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0213E", \$defaultText, @_);
}


sub INET_FTP_READER_OUTPUT_PARAM_CHECK_1()
{
   my $defaultText = <<'::STOP::';
The error output port must have one attribute of type rstring.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0214E", \$defaultText, @_);
}


sub INET_NONZERO_LIBCURL_RC($$$)
{
   my $defaultText = <<'::STOP::';
A Uniform Resource Identifier retrieval from {0} was not successful. The numeric return code from the libcurl agent was {1,number,integer} and the error message text was: {2}. The Processing Element will continue running.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0230W", \$defaultText, @_);
}


sub INET_NONZERO_LIBCURL_RC_REPEATED($$$$)
{
   my $defaultText = <<'::STOP::';
A Uniform Resource Identifier retrieval from {0} has not been successful for the last {3,number,integer} attempts. The numeric return code from the libcurl agent was {1,number,integer} and the error message text was: {2}. The Processing Element will continue running.
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0231W", \$defaultText, @_);
}


sub INET_DEPRECATED_COF($)
{
   my $defaultText = <<'::STOP::';
Deprecated custom output function used: ''{0}''
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0232W", \$defaultText, @_);
}


sub INET_CONNECTION_SUCCESS($)
{
   my $defaultText = <<'::STOP::';
Connection successful ''{0}''
::STOP::
    return SPL::Helper::SPLFormattedMessageWithResname($toolkitRoot, "com.ibm.streamsx.inet", "InetResource", "en_US/InetResource.xlf", "CDIST0240I", \$defaultText, @_);
}

1;
