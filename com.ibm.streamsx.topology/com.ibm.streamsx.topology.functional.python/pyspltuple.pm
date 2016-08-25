# begin_generated_IBM_copyright_prolog                             
#                                                                  
# This is an automatically generated copyright prolog.             
# After initializing,  DO NOT MODIFY OR MOVE                       
# **************************************************************** 
# Licensed Materials - Property of IBM                             
# 5724-Y95                                                         
# (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.      
# US Government Users Restricted Rights - Use, duplication or      
# disclosure restricted by GSA ADP Schedule Contract with          
# IBM Corp.                                                        
#                                                                  
# end_generated_IBM_copyright_prolog                               
sub convertAndAddToPythonDictionaryObject {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];

  my $get = '{ PyObject * pyValue = ';
  $get = $get . convertToPythonValue($ituple, $type, $name);

  $getkey = '{ PyObject * pyDictKey = PyUnicode_DecodeUTF8((const char*)  "' . $name . '", ((int)(sizeof("' . $name . '")))-1 , NULL);'."\n";

# Note PyDict_SetItem does not steal the references to the key and value
  my $setdict =  "  PyDict_SetItem(pyDict, pyDictKey, pyValue);\n";
  $setdict =  $setdict . "  Py_DECREF(pyDictKey);\n";
  $setdict =  $setdict . "  Py_DECREF(pyValue);}}\n";

  return $get . $getkey . $setdict ;
}

1;
