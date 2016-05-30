use Switch;

# This function does the reverse, converting a Python type back to a
# c++ type based on the $type argument which is a string literal.
#
sub pythonToCppPrimitiveConversion{
# TODO: do error checking for the conversions. E.g., 
# char * str = PyUnicode_AsUTF8(pyAttrValue)
# if(str==NULL) exit(0);
 
  my ($convert_from_string, $type) = @_;
    switch ($type) {
             case 'rstring' {return "SPL::rstring( PyUnicode_AsUTF8($convert_from_string))";}
             case 'ustring' {return "SPL::ustring::fromUTF8( PyUnicode_AsUTF8($convert_from_string))";}
             case 'int8' {return "(int8_t) PyLong_AsLong($convert_from_string)";}
             case 'int16' {return "(int16_t) PyLong_AsLong($convert_from_string)";}
             case 'int32' {return "(int32_t) PyLong_AsLong($convert_from_string)";}
             case 'int64' {return "PyLong_AsLong($convert_from_string)";}
             case 'uint8' {return "(uint8_t) PyLong_AsUnsignedLong($convert_from_string)";}
             case 'uint16' {return "(uint16_t) PyLong_AsUnsignedLong($convert_from_string)";}
             case 'uint32' {return "(uint32_t) PyLong_AsUnsignedLong($convert_from_string)";}
             case 'uint64' {return "PyLong_AsUnsignedLong($convert_from_string)";}
             case 'float32' {return "(float) PyFloat_AsDouble($convert_from_string)";}
             case 'float64' {return "PyFloat_AsDouble($convert_from_string)";}
             case 'boolean' {return "PyObject_IsTrue($convert_from_string)";}
             case 'complex32' { return "SPL::complex32((float32_t) PyComplex_RealAsDouble($convert_from_string), (float32_t) PyComplex_ImagAsDouble($convert_from_string))";}
             case 'complex64' { return "SPL::complex64(PyComplex_RealAsDouble($convert_from_string), PyComplex_ImagAsDouble($convert_from_string))";}
	     else {SPL::CodeGen::exitln("An unknown type $type was encountered when converting to back to cpp types."); }
    }
}

sub convertToPythonDictionaryObject {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];


  my $pyTypeConv = "UNKNOWN_TYPE";
  my $get = undef;
  my $getval = undef;
  my $setdict = undef;

  # input value
#   my $iv = $ituple . ".get_" . $name . "()";
  my $iv = "ip.get_" . $name . "()";
#
  # If the type is a list, get the element type and make the
  # corresponding Python type. The List needsto be iterated through at
  # runtime becaues it could be of variable length.
  if (SPL::CodeGen::Type::isList($type)) {
      my $size = $iv . ".size()";
      my $loop = "for(int i = 0; i < $size; i++){\n";
      $get = "pyValue = PyList_New($size);\n";
      my $element_type = SPL::CodeGen::Type::getElementType($type);
      $loop = $loop . "PyObject *o =" . cppToPythonPrimitiveConversion("($iv)[i]", $element_type) . ";\n";      
      $loop = $loop . "PyList_SetItem(pyValue, i, o);\n";
      $loop = $loop . "}\n";
      $get = $get . $loop;
  }  
  elsif(SPL::CodeGen::Type::isSet($type)){      
# my $value_type = SPL::CodeGen::Type::getValueType($type);
      my $element_type = SPL::CodeGen::Type::getElementType($type);

      $get = "pyValue = PySet_New(NULL);\n";

      my $loop = "for(std::tr1::unordered_set<SPL::$element_type>::const_iterator it = $iv.begin();\n";
      $loop = $loop . "it!=$iv.end(); it++){\n";
      $loop = $loop . "PyObject *v = " . cppToPythonPrimitiveConversion("*it", $element_type) . ";\n";
      $loop = $loop . "PySet_Add(pyValue, v);\n";
      $loop = $loop . "}";
      $get = $get . $loop;
  }
  # If the type is a map, again, get the key and value types, then
  # iterate through the map to copy its contents.
  elsif(SPL::CodeGen::Type::isMap($type)){      
      my $key_type = SPL::CodeGen::Type::getKeyType($type);
      my $value_type = SPL::CodeGen::Type::getValueType($type);

      $get = "pyValue = PyDict_New();\n";

      my $loop = "for(std::tr1::unordered_map<SPL::$key_type,SPL::$value_type>::const_iterator it = $iv.begin();\n";
      $loop = $loop . "it!=$iv.end(); it++){\n";
      $loop = $loop . "PyObject *k = " . cppToPythonPrimitiveConversion("it->first", $key_type) . ";\n";
      $loop = $loop . "PyObject *v = " . cppToPythonPrimitiveConversion("it->second", $value_type) . ";\n";
      # Note PyDict_SetItem does not steal the referenceis to the key and value
      $loop = $loop . "PyDict_SetItem(pyValue, k, v);\n";
      $loop = $loop . "Py_DECREF(k);\n";
      $loop = $loop . "Py_DECREF(v);\n";
      $loop = $loop . "}";
      $get = $get . $loop;
  }
  # Must be primitive type
  else {

    my $key_type = SPL::CodeGen::Type::getKeyType($type);
    my $value_type = SPL::CodeGen::Type::getValueType($type);

    my $assign = undef;
    $getval = "  pyValue = " . cppToPythonPrimitiveConversion($iv, $type) . ";\n";
  }
  $getkey = 'pyDictKey = PyUnicode_DecodeUTF8((const char*)  "' . $name . '", ((int)(sizeof("' . $name . '")))-1 , NULL);'."\n";

# Note PyDict_SetItem does not steal the references to the key and value
  $setdict =  "  PyDict_SetItem(pyDict, pyDictKey, pyValue);\n";
  $setdict =  $setdict . "  Py_DECREF(pyDictKey);\n";
  $setdict =  $setdict . "  Py_DECREF(pyValue);\n";

  return $get . $assign . $getval . $getkey . $setdict ;
}

1;
