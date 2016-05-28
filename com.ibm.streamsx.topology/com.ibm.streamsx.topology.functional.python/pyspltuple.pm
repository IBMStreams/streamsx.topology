use Switch;

# This function takes the string of a value to be converted and its
# type, and generates the corresponding code to convert the type from
# c++ to Python. Note that the $type argument is a SPL::CodeGen type,
# and not a string literal
#
sub cppToPythonPrimitiveConversion{
# TODO: do error checking for the conversions. E.g., 
# char * str = PyUnicode_AsUTF8(pyAttrValue)
# if(str==NULL) exit(0);
#
# (or the equivalent for this function)

    my ($convert_from_string, $type) = @_;

    my $supported = 0;

    if(SPL::CodeGen::Type::isSigned($type)) {
      return "PyLong_FromLong($convert_from_string)";
    } 
    elsif(SPL::CodeGen::Type::isUnsigned($type)) {
      return "PyLong_FromUnsignedLong($convert_from_string)";
    } 
    elsif(SPL::CodeGen::Type::isFloatingpoint($type)) {
      $supported = 1;
    } 
    elsif (SPL::CodeGen::Type::isRString($type) || SPL::CodeGen::Type::isBString($type)) {
      $supported = 1;
    } 
    elsif (SPL::CodeGen::Type::isUString($type)) {
      $supported = 1;
    } 
    elsif(SPL::CodeGen::Type::isBoolean($type)) {
      $supported = 1;
    } 
    elsif (SPL::CodeGen::Type::isComplex32($type) || SPL::CodeGen::Type::isComplex64($type)) {
      $supported = 1;
    }

    if ($supported == 1) {
      return "streamsx::topology::pyAttributeToPyObject($convert_from_string)";
    }
    else{
      SPL::CodeGen::errorln("An unknown type was encountered when converting to python types." . $type ); 
    }
}

sub rstring2python {
    my ($convert_from_string) = @_;
    return 'PyUnicode_DecodeUTF8((const char*)  (' . $convert_from_string . ".data()), " . $convert_from_string . ".size(), NULL)";
}
sub ustring2python {
    my ($convert_from_string) = @_;
    return 'PyUnicode_DecodeUTF16((const char*)  (' . $convert_from_string . ".getBuffer()), " . $convert_from_string . ".length()*2, NULL, NULL)";
}

# This function is identical to the cppToPythonPrimitiveConversion,
# except the type of the object to be converted is passed as a string,
# instead of an SPL::CodeGen type. This is necessary because there are
# functions in the CodeGen API that return strings and not a CodeGen
# types, such as OutputPort::getAttributeAt().
#
sub stringBasedCppToPythonPrimitiveConversion{
# TODO: do error checking for the conversions. E.g., 
# char * str = PyUnicode_AsUTF8(pyAttrValue)
# if(str==NULL) exit(0);
#
# (or the equivalent for this function)

    my ($convert_from_string, $type) = @_;
 SPL::CodeGen::errorln("smd2 -- " . $convert_from_string . " : " . $type);
    switch ($type) {
      case ['int8', 'int16', 'int32', 'int64'] { return "PyLong_FromLong($convert_from_string)";}
      case ['uint8', 'uint16', 'uint32', 'uint64'] { return "PyLong_FromUnsignedLong($convert_from_string)";}
      case ['float32', 'float64'] { return "PyFloat_FromDouble($convert_from_string)";}
      case 'rstring' { return rstring2python($convert_from_string);}
      case 'ustring' { return ustring2python($convert_from_string);}
      case 'boolean' { return "$convert_from_string ? Py_True : Py_False; Py_INCREF(pyValue)";}
      case ['complex32', 'complex64'] { return "PyComplex_FromDoubles(". $convert_from_string . ".real(), ". $convert_from_string . ".imag())";}
      else { SPL::CodeGen::errorln("SMD2 An unknown type was encountered when converting to python types: $type"); }
    }
}

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


#
# Return a C++ statement converting a input attribute
# from an SPL input tuple to a Python object and
# setting it into pyTuple (as a Python Tuple).
# Assumes a C++ variable pyValue and pyTuple are defined.
#
sub convertToPythonValue {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];


  my $pyTypeConv = "UNKNOWN_TYPE";
  my $get = undef;

  # input value
  my $iv = $ituple . ".get_" . $name . "()";
  
  # If the type is a list, get the element type and make the
  # corresponding Python type. The List needsto be iterated through at
  # runtime becaues it could be of variable length.
  if (SPL::CodeGen::Type::isList($type)) {
      my $size = $iv . ".size()";
      my $loop = "for(int i = 0; i < $size; i++){\n";
      $get = "pyValue = PyList_New($size);\n";
      my $element_type = SPL::CodeGen::Type::getElementType($type);
      $loop = $loop . "PyObject *o =" . stringBasedCppToPythonPrimitiveConversion("($iv)[i]", $element_type) . ";\n";      
      $loop = $loop . "PyList_SetItem(pyValue, i, o);\n";
      $loop = $loop . "}\n";
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
      $loop = $loop . "PyObject *k = " . stringBasedCppToPythonPrimitiveConversion("it->first", $key_type) . ";\n";
      $loop = $loop . "PyObject *v = " . stringBasedCppToPythonPrimitiveConversion("it->second", $value_type) . ";\n";
      $loop = $loop . "PyDict_SetItem(pyValue, k, v);\n";
      $loop = $loop . "}";
      $get = $get . $loop;
  }

  # Must be primitive type
  else{
      $get = "pyValue = " . cppToPythonPrimitiveConversion($iv, $type) . ";\n";
  }
  
  # Note PyTuple_SetItem steals the reference to the value
  my $assign =  "    PyTuple_SetItem(pyTuple, " . $i  .", pyValue);\n";

  return $get . $assign ;
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
