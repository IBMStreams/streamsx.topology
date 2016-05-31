use Switch;

sub convertToPythonDictionaryObject {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];


  my $pyTypeConv = "UNKNOWN_TYPE";
  my $get = undef;

  # input value
#   my $iv = $ituple . ".get_" . $name . "()";
  my $iv = "ip.get_" . $name . "()";
#
  # If the type is a list, get the element type and make the
  # corresponding Python type. The List needsto be iterated through at
  # runtime becaues it could be of variable length.
  if (SPL::CodeGen::Type::isList($type)) {
      $get = cppToPythonListConversion($iv, $type);
  }  
  elsif(SPL::CodeGen::Type::isSet($type)){      
# my $value_type = SPL::CodeGen::Type::getValueType($type);
      my $element_type = SPL::CodeGen::Type::getElementType($type);

      $get = "pyValue = PySet_New(NULL);\n";

      my $loop = "for(std::tr1::unordered_set<SPL::$element_type>::const_iterator it = $iv.begin();\n";
      $loop = $loop . "it!=$iv.end(); it++){\n";
      $loop = $loop . "PyObject *v = " . cppToPythonPrimitiveConversion("*it", $element_type) . ";\n";
      $loop = $loop . "PySet_Add(pyValue, v);\n";
      $loop = $loop . "Py_DECREF(v);\n";
      $loop = $loop . "}";
      $get = $get . $loop;
  }
  # If the type is a map, again, get the key and value types, then
  # iterate through the map to copy its contents.
  elsif(SPL::CodeGen::Type::isMap($type)){      
      $get = cppToPythonMapConversion($iv, $type);
  }
  # Must be primitive type
  else {
    $get = "  pyValue = " . cppToPythonPrimitiveConversion($iv, $type) . ";\n";
  }
  $getkey = 'pyDictKey = PyUnicode_DecodeUTF8((const char*)  "' . $name . '", ((int)(sizeof("' . $name . '")))-1 , NULL);'."\n";

# Note PyDict_SetItem does not steal the references to the key and value
  my $setdict =  "  PyDict_SetItem(pyDict, pyDictKey, pyValue);\n";
  $setdict =  $setdict . "  Py_DECREF(pyDictKey);\n";
  $setdict =  $setdict . "  Py_DECREF(pyValue);\n";

  return $get . $getkey . $setdict ;
}

1;
