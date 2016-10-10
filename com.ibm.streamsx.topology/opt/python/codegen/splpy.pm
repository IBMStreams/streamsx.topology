
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

# This function does the reverse, converting a Python type back to a
# c++ type based on the $type argument which is a string literal.
#
# $convert_from_string - C++ expression representing PyObject * 
# $type - SPL type of target attribute
#
#
sub pythonToCppPrimitiveConversion{
 
  my ($convert_from_string, $type) = @_;
  if ($type eq 'rstring') {
    return "SPL::rstring( PyUnicode_AsUTF8($convert_from_string))";
  }
  elsif ($type eq 'ustring') {
    return "SPL::ustring::fromUTF8( PyUnicode_AsUTF8($convert_from_string))";
  }
  elsif ($type eq 'int8') {
    return "(int8_t) PyLong_AsLong($convert_from_string)";
  }
  elsif ($type eq 'int16') {
    return "(int16_t) PyLong_AsLong($convert_from_string)";
  }
  elsif ($type eq 'int32') {
    return "(int32_t) PyLong_AsLong($convert_from_string)";
  }
  elsif ($type eq 'int64') {
    return "PyLong_AsLong($convert_from_string)";
  }
  elsif ($type eq 'uint8') {
    return "(uint8_t) PyLong_AsUnsignedLong($convert_from_string)";
  }
  elsif ($type eq 'uint16') {
    return "(uint16_t) PyLong_AsUnsignedLong($convert_from_string)";
  }
  elsif ($type eq 'uint32') {
    return "(uint32_t) PyLong_AsUnsignedLong($convert_from_string)";
  }
  elsif ($type eq 'uint64') {
    return "PyLong_AsUnsignedLong($convert_from_string)";
  }
  elsif ($type eq 'float32') {
    return "(float) PyFloat_AsDouble($convert_from_string)";
  }
  elsif ($type eq 'float64') {
    return "PyFloat_AsDouble($convert_from_string)";
  }
  elsif ($type eq 'boolean') {
    return "PyObject_IsTrue($convert_from_string)";
  }
  elsif ($type eq 'complex32') {
    return "SPL::complex32((SPL::float32) PyComplex_RealAsDouble($convert_from_string), (SPL::float32) PyComplex_ImagAsDouble($convert_from_string))";
  }
  elsif ($type eq 'complex64') {
    return "SPL::complex64(PyComplex_RealAsDouble($convert_from_string), PyComplex_ImagAsDouble($convert_from_string))";
  }
  else {
    SPL::CodeGen::exitln("An unknown type $type was encountered when converting to back to cpp types.");
  }
}

sub cppToPythonListConversion {

    my ($iv, $type) = @_;

      my $element_type = SPL::CodeGen::Type::getElementType($type);

      my $size = $iv . ".size()";
      my $get = "PyList_New($size);\n";

      my $loop = "for(int i = 0; i < $size; i++){\n";
      $loop = $loop . "PyObject *o =" . cppToPythonPrimitiveConversion("($iv)[i]", $element_type) . ";\n";      
      $loop = $loop . "PyList_SetItem(pyValue, i, o);\n";
      $loop = $loop . "}\n";

      $get = $get . $loop;
      return $get;
}

sub cppToPythonMapConversion {
      my ($iv, $type) = @_;

      my $key_type = SPL::CodeGen::Type::getKeyType($type);
      my $value_type = SPL::CodeGen::Type::getValueType($type);

      my $get = "PyDict_New();\n";

      my $loop = "for(std::tr1::unordered_map<SPL::$key_type,SPL::$value_type>::const_iterator it = $iv.begin();\n";
      $loop = $loop . "it!=$iv.end(); it++){\n";
      $loop = $loop . "PyObject *k = " . cppToPythonPrimitiveConversion("it->first", $key_type) . ";\n";
      $loop = $loop . "PyObject *v = " . cppToPythonPrimitiveConversion("it->second", $value_type) . ";\n";
      $loop = $loop . "PyDict_SetItem(pyValue, k, v);\n";
      $loop =  $loop . "  Py_DECREF(k);\n";
      $loop =  $loop . "  Py_DECREF(v);\n";
      $loop = $loop . "}";
      $get = $get . $loop;

      return $get;
}

sub cppToPythonSetConversion {
      my ($iv, $type) = @_;

      my $element_type = SPL::CodeGen::Type::getElementType($type);

      $get = "PySet_New(NULL);\n";

      my $loop = "for(std::tr1::unordered_set<SPL::$element_type>::const_iterator it = $iv.begin();\n";
      $loop = $loop . "it!=$iv.end(); it++){\n";
      $loop = $loop . "PyObject *v = " . cppToPythonPrimitiveConversion("*it", $element_type) . ";\n";
      $loop = $loop . "PySet_Add(pyValue, v);\n";
      $loop = $loop . "Py_DECREF(v);\n";
      $loop = $loop . "}";
      $get = $get . $loop;

      return $get;
}

#
# Return a C++ statement converting a input attribute
# from an SPL input tuple to a Python object
# Assumes a C++ variable pyValue is defined.
#
sub convertToPythonValue {
  my $ituple = $_[0];
  my $type = $_[1];
  my $name = $_[2];

  # input value
  my $iv = $ituple . ".get_" . $name . "()";

  return convertToPythonValueFromExpr($type, $iv);
}

##
## Convert to a Python value from an expression
##
sub convertToPythonValueFromExpr {
  my $type = $_[0];
  my $iv = $_[1];
  
  # If the type is a list, get the element type and make the
  # corresponding Python type. The List needsto be iterated through at
  # runtime becaues it could be of variable length.
  if (SPL::CodeGen::Type::isList($type)) {
      return cppToPythonListConversion($iv, $type);
  }  
  # If the type is a set, get the value type, then
  # iterate through the set to copy its contents.
  elsif(SPL::CodeGen::Type::isSet($type)){      
      return cppToPythonSetConversion($iv, $type);
  }
  # If the type is a map, again, get the key and value types, then
  # iterate through the map to copy its contents.
  elsif(SPL::CodeGen::Type::isMap($type)){      
      return cppToPythonMapConversion($iv, $type);
  }
  elsif(SPL::CodeGen::Type::isPrimitive($type)) { 
      return cppToPythonPrimitiveConversion($iv, $type) . ";\n";
  }
  else {
      SPL::CodeGen::errorln("An unknown type was encountered when converting to python types." . $type ); 
  }
}

#
# Return a C++ statement converting a input attribute
# from an SPL input tuple to a Python object and
# setting it into pyTuple (as a Python Tuple).
# Assumes a C++ variable pyValue and pyTuple are defined.
#
sub convertToPythonValueAsTuple {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];

  my $get = "pyValue = " . convertToPythonValue($ituple, $type, $name);
  
  # Note PyTuple_SetItem steals the reference to the value
  my $assign =  "    PyTuple_SetItem(pyTuple, " . $i  .", pyValue);\n";

  return $get . $assign ;
}

# Determine which style of argument is being
# used. These match the SPL types in
# com.ibm.streamsx.topology/types.spl
#
# SPL TYPE - style - comment
# blob __spl_po - python - pickled Python object
# rstring string - string - SPL rstring
# rstring jsonString - json - JSON as SPL rstring
# xml document - xml - XML document
# blob binary - binary - Binary data
#
# tuple<...> - dict - Any SPL tuple type apart from above
#
# Not all are supported yet.
# 

sub splpy_tuplestyle{

 my ($port) = @_;

 my $attr =  $port->getAttributeAt(0);
 my $attrtype = $attr->getSPLType();
 my $attrname = $attr->getName();
 my $pystyle = 'unk';
 my $numattrs = $port->getNumberOfAttributes();

 if (($numattrs == 1) && SPL::CodeGen::Type::isBlob($attrtype) && ($attrname eq '__spl_po')) {
    $pystyle = 'pickle';
 } elsif (($numattrs == 1) && SPL::CodeGen::Type::isRString($attrtype) && ($attrname eq 'string')) {
    $pystyle = 'string';
 } elsif (($numattrs == 1) && SPL::CodeGen::Type::isRString($attrtype) && ($attrname eq 'jsonString')) {
    $pystyle = 'json';
 } elsif (($numattrs == 1) && SPL::CodeGen::Type::isBlob($attrtype) && ($attrname eq 'binary')) {
    $pystyle = 'binary';
    SPL::CodeGen::errorln("Blob schema is not currently supported for Python."); 
 } elsif (($numattrs == 1) && SPL::CodeGen::Type::isXml($attrtype) && ($attrname eq 'document')) {
    $pystyle = 'xml';
    SPL::CodeGen::errorln("XML schema is not currently supported for Python."); 
 } else {
    $pystyle = 'dict';
 }

 return $pystyle;
}

# Given a style return a string containing
# the C++ code to get the value
# from an input tuple ip, that will
# be converted to Python and passed to the function.
#
# Must setup a C++ variable called 'value' that
# represents the value to be passed into the Python function
#
sub splpy_inputtuple2value{
 my ($pystyle) = @_;
 if ($pystyle eq 'pickle') {
  return 'SPL::blob const & value = ip.get___spl_po();';
 }

 if ($pystyle eq 'string') {
  return 'SPL::rstring const & value = ip.get_string();';
 }
 
 if ($pystyle eq 'json') {
  return 'SPL::rstring const & value = ip.get_jsonString();';
 }

 if ($pystyle eq 'dict') {
  # nothing done here for dict style 
 }
}

#
# Convert attribute of an SPL tuple to Python
# and add to a dictionary object.
#
# ituple - C++ expression of the tuple
# i  - Attribute index
# type - spl type
# name - attribute name
# names - PyObject * pointing to Python tuple containing attribute names.

sub convertAndAddToPythonDictionaryObject {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];
  my $names = $_[4];

  my $get = '{ PyObject * pyValue = ';
  $get = $get . convertToPythonValue($ituple, $type, $name);

  # PyTuple_GET_ITEM returns a borrowed reference.
  $getkey = '{ PyObject * pyDictKey = PyTuple_GET_ITEM(' . $names . ',' . $i . ") ;\n";

# Note PyDict_SetItem does not steal the references to the key and value
  my $setdict =  "  PyDict_SetItem(pyDict, pyDictKey, pyValue);\n";
  $setdict =  $setdict . "  Py_DECREF(pyValue);}}\n";

  return $get . $getkey . $setdict ;
}

1;
