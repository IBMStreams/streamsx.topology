
# Check if a SPL type is supported for conversion
# to/from a Python value.
#
sub splToPythonConversionCheck{

    my ($type) = @_;

    if (SPL::CodeGen::Type::isList($type)) {
        my $element_type = SPL::CodeGen::Type::getElementType($type);
        splToPythonConversionCheck($element_type);
        return;
    }
    elsif (SPL::CodeGen::Type::isSet($type)) {
        my $element_type = SPL::CodeGen::Type::getElementType($type);
        # Python sets must have hashable keys
        # (which excludes Python collection type such as list,map,set)
        # so for now restrict to primitive types)
        #
        # blob is excluded as the value can become
        # invalid while in a set which will likely break
        if (SPL::CodeGen::Type::isPrimitive($element_type)
          && ! SPL::CodeGen::Type::isBlob($element_type)) {
            splToPythonConversionCheck($element_type);
            return;
        }
    }
    elsif (SPL::CodeGen::Type::isMap($type)) {
        my $key_type = SPL::CodeGen::Type::getKeyType($type);
        # Python maps must have hashable keys
        # (which excludes Python collection type such as list,map,set)
        # so for now restrict to primitive types)
        #
        # blob is excluded as the value can become
        # invalid while in a map which will likely break as a key
        if (SPL::CodeGen::Type::isPrimitive($key_type)
          && ! SPL::CodeGen::Type::isBlob($key_type)) {
            splToPythonConversionCheck($key_type);

           my $value_type = SPL::CodeGen::Type::getValueType($type);
           splToPythonConversionCheck($value_type);
           return;
        }
    }
    elsif(SPL::CodeGen::Type::isSigned($type)) {
      return;
    } 
    elsif(SPL::CodeGen::Type::isUnsigned($type)) {
      return;
    } 
    elsif(SPL::CodeGen::Type::isFloat($type)) {
      return;
    } 
    elsif(SPL::CodeGen::Type::isDecimal($type)) {
      return;
    } 
    elsif (SPL::CodeGen::Type::isRString($type) || SPL::CodeGen::Type::isBString($type)) {
      return;
    } 
    elsif (SPL::CodeGen::Type::isUString($type)) {
      return;
    } 
    elsif(SPL::CodeGen::Type::isBoolean($type)) {
      return;
    } 
    elsif(SPL::CodeGen::Type::isTimestamp($type)) {
      return;
    }
    elsif(SPL::CodeGen::Type::isBlob($type)) {
      return;
    }
    elsif (SPL::CodeGen::Type::isComplex32($type) || SPL::CodeGen::Type::isComplex64($type)) {
      return;
    }
    elsif(hasOptionalTypesSupport() && SPL::CodeGen::Type::isOptional($type)) {
      my $value_type = SPL::CodeGen::Type::getUnderlyingType($type);
      splToPythonConversionCheck($value_type);
      return;
    }
    if (SPL::CodeGen::Type::isTuple($type)) {
      return; # __NESTED_TUPLE__
    }
    SPL::CodeGen::errorln("SPL type: " . $type . " is not supported for conversion to or from Python."); 
}

#
# Return a C++ expression converting a input attribute
# from an SPL input tuple to a Python object
#
sub convertAttributeToPythonValue {
  my $ituple = $_[0];
  my $type = $_[1];
  my $name = $_[2];

  # input value
  my $iv = $ituple . ".get_" . $name . "()";

  return convertToPythonValueFromExpr($type, $iv);
}

##
## Convert to a Python value from an expression
## Returns a string with a C++ expression
## representing the Python value
##
sub convertToPythonValueFromExpr {
  my $type = $_[0];
  my $iv = $_[1];

  # Check the type is supported
  splToPythonConversionCheck($type);

  return "streamsx::topology::pySplValueToPyObject($iv)";
}

# Check if a type includes blobs in its definition.
# Could be just blob, or list<blob> etc.
# blob is not supported for map keys or set values.
sub typeHasBlobs {
  my $type = $_[0];

  if (SPL::CodeGen::Type::isBlob($type)) {
      return 1;
  }
  if (SPL::CodeGen::Type::isList($type)) {
      my $element_type = SPL::CodeGen::Type::getElementType($type);
      return typeHasBlobs($element_type);
  }
  if (SPL::CodeGen::Type::isMap($type)) {
      my $value_type = SPL::CodeGen::Type::getValueType($type);
      return typeHasBlobs($value_type);
  }
  if(hasOptionalTypesSupport() && SPL::CodeGen::Type::isOptional($type)) {
      my $value_type = SPL::CodeGen::Type::getUnderlyingType($type);
      return typeHasBlobs($value_type);
  }

  return 0;
}

#
# Return a C++ code block converting a input attribute
# from an SPL input tuple to a Python object and
# setting it into pyTuple (as a Python Tuple).
# Assumes a C++ variable pyTuple is defined.
#
sub convertToPythonValueAsTuple {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];
  
  # starts a C++ block and sets pyValue
  my $get = _attr2Value($ituple, $type, $name);

  # Note PyTuple_SET_ITEM steals the reference to the value
  my $assign =  "PyTuple_SET_ITEM(pyTuple, $i, value);\n";

  return $get . $assign . "}\n" ;
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
 my ($pystyle, $iport) = @_;
 if ($pystyle eq 'pickle') {
  return 'SPL::blob const & value = ' . $iport->getCppTupleName() . '.get___spl_po();';
 }

 if ($pystyle eq 'string') {
  return 'SPL::rstring const & value = ' . $iport->getCppTupleName() . '.get_string();';
 }
 
 if ($pystyle eq 'json') {
  return 'SPL::rstring const & value = ' . $iport->getCppTupleName() . '.get_jsonString();';
 }

 if ($pystyle eq 'dict') {
  # nothing done here for dict style 
  # instead cgt is used to generate code specific
  # the input schema
 }
}

# Starts a block that converts an SPL attribute
# to the enclosed variable value
sub _attr2Value {
  my $ituple = $_[0];
  my $type = $_[1];
  my $name = $_[2];
  my $spaces = $_[3];

  my $get = "".$spaces."{\n";
  $get = $get . "".$spaces."  PyObject * value = ";
  $get = $get . convertAttributeToPythonValue($ituple, $type, $name);
  $get = $get . ";\n";

  # If the attribute has blobs then
  # save the corresponding memory view objects.
  # We just save the attribute's Python representation
  # Typically, it will be a memoryview (from a blob) but
  # if it's something containing blobs the complete collection
  # object is passed to Python for release.
  #
  # Assumes that pyMvs exists set up by py_splTupleCheckForBlobs.cgt
  if (typeHasBlobs($type)) {
      $get = $get .$spaces."  PYSPL_MEMORY_VIEW(value);\n";
  }
  return $get;
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
  my $dictname = $_[5];
  my $spaces = $_[6];
  my $output_dir = $_[7];
  
  # starts a C++ block and sets value
  my $get;
  my $nested_tuple = 0;
  ########### SPL: Map of tuple #########################
  if (SPL::CodeGen::Type::isMap($type)) {  
    if (SPL::CodeGen::Type::isTuple(SPL::CodeGen::Type::getValueType($type))) {
      $nested_tuple = 1;
      if ($ituple eq 'it->second') {
        SPL::CodeGen::errorln("SPL type: " . $type . " is not supported for conversion to Python.");
      }
      $get = "". $spaces."{\n";
      $get = $get . $spaces."  // SPL Map with tuple as value type\n";
      $get = $get . $spaces."  // key type: ".SPL::CodeGen::Type::getKeyType($type)."\n";    
      $get = $get . $spaces."  // value type: ".SPL::CodeGen::Type::getValueType($type)."\n";
      $get = $get . $spaces."  PyObject * value = 0;\n";
      $get = $get . $spaces."  {\n";
      $get = $get . $spaces."    // SPL Map Conversion to Python dict\n";
      $get = $get . $spaces."    PyObject * pyDict1 = PyDict_New();\n";
      $get = $get . $spaces."    for (typename std::tr1::unordered_map<".SPL::CodeGen::Type::getKeyType($type).",".spl_cpp_type($name, 'SPL::map', SPL::CodeGen::Type::getValueType($type), $output_dir).">::const_iterator it = $ituple.get_$name().begin(); it != $ituple.get_$name().end(); it++) {\n";
      $get = $get . $spaces."      PyObject *k = pySplValueToPyObject(it->first);\n";
      $get = $get . $spaces."      PyObject *value = 0;\n";      
      $get = $get . $spaces."      {\n";
      $get = $get . $spaces."        PyObject * pyDict1 = PyDict_New();\n";
      my $ac = 0;
      for my $_attrName (SPL::CodeGen::Type::getAttributeNames (SPL::CodeGen::Type::getValueType($type))) {  	
        $ac++;
      }
      $get = $get . $spaces."        PyObject * pyNamesNestedTuple = PyTuple_New(".$ac.");\n";
      my $j = 0;
      for my $_attrName (SPL::CodeGen::Type::getAttributeNames (SPL::CodeGen::Type::getValueType($type))) {   
        $get = $get . $spaces."        PyObject * pyName".$j." = pyUnicode_FromUTF8(\"".$_attrName."\");\n"; 
        $get = $get . $spaces."        PyTuple_SET_ITEM(pyNamesNestedTuple, ".$j.", pyName".$j.");\n";
        $j++;
      }
      my @attrTypes = SPL::CodeGen::Type::getAttributeTypes (SPL::CodeGen::Type::getValueType($type));
      my @attrNames = SPL::CodeGen::Type::getAttributeNames (SPL::CodeGen::Type::getValueType($type)); 
      for (my $attr_index = 0; $attr_index < $ac; ++$attr_index) {
        $get = $get . convertAndAddToPythonDictionaryObject('it->second', $attr_index, $attrTypes[$attr_index], $attrNames[$attr_index], 'pyNamesNestedTuple', 'pyDict1', $spaces."        ", $output_dir);
      }
      $get = $get . $spaces."        value = pyDict1;\n";
      $get = $get . $spaces."      }\n";
      $get = $get . $spaces."      PyDict_SetItem(pyDict1, k, value);\n";
      $get = $get . $spaces."      Py_DECREF(k);\n";
      $get = $get . $spaces."      Py_DECREF(value);\n";
      $get = $get . $spaces."    }\n";      
      $get = $get . $spaces."    value = pyDict1;\n";
      $get = $get . $spaces."  }\n";
    } elsif (SPL::CodeGen::Type::isList(SPL::CodeGen::Type::getValueType($type))) {
      my $element_type = SPL::CodeGen::Type::getElementType(SPL::CodeGen::Type::getValueType($type));  
      if (SPL::CodeGen::Type::isTuple($element_type)) {
      	$nested_tuple = 1;
      	SPL::CodeGen::errorln("SPL type: " . $type . " is not supported for conversion to Python.");
      $get = "". $spaces."{\n";
      $get = $get . $spaces."  // SPL Map with list of tuple as value type\n";
      $get = $get . $spaces."  // key type: ".SPL::CodeGen::Type::getKeyType($type)."\n";    
      $get = $get . $spaces."  // value type: ".SPL::CodeGen::Type::getValueType($type)."\n";
      $get = $get . $spaces."  PyObject * value = 0;\n";
      $get = $get . $spaces."  {\n";
      $get = $get . $spaces."    // SPL Map Conversion to Python dict\n";
      $get = $get . $spaces."    PyObject * pyDict1 = PyDict_New();\n";
      $get = $get . $spaces."    value = pyDict1;\n";
      $get = $get . $spaces."  }\n";
      }
    }
  }
  ########### SPL: list of tuple --> PyList of PyDict #########################
  if (SPL::CodeGen::Type::isList($type)) {
    my $element_type = SPL::CodeGen::Type::getElementType($type);  
    if (SPL::CodeGen::Type::isTuple($element_type)) {
      $nested_tuple = 1;
      $get = "". $spaces."{\n";
      $get = $get . $spaces."  // SPL: list of tuple --> PyList of PyDict\n";
      $get = $get . $spaces."  // $type $name\n";
      $get = $get . $spaces."  PyObject * value = 0;\n";
      $get = $get . $spaces."  {\n";
      $get = $get . $spaces."    PyObject * pyList = PyList_New($ituple.get_$name().size());\n";
      my $ac = 0;
      for my $_attrName (SPL::CodeGen::Type::getAttributeNames ($element_type)) {  	
        $ac++;
      }
      $get = $get . $spaces."    PyObject * pyNamesNestedTuple = PyTuple_New(".$ac.");\n";
      my $j = 0;
      for my $_attrName (SPL::CodeGen::Type::getAttributeNames ($element_type)) {   
      	$get = $get . $spaces."    PyObject * pyName".$j." = pyUnicode_FromUTF8(\"".$_attrName."\");\n"; 
        $get = $get . $spaces."    PyTuple_SET_ITEM(pyNamesNestedTuple, ".$j.", pyName".$j.");\n";
        $j++;
      }
      $get = $get . $spaces."    for (int list_index = 0; list_index < $ituple.get_$name().size(); list_index++) {\n";
      $get = $get . $spaces."      // PyDict\n";
      $get = $get . $spaces."      PyObject * value = 0;\n";
      $get = $get . $spaces."      {\n";
      $get = $get . $spaces."        PyObject * pyDict1 = PyDict_New();\n";
      my @attrTypes = SPL::CodeGen::Type::getAttributeTypes ($element_type);
      my @attrNames = SPL::CodeGen::Type::getAttributeNames ($element_type); 
      for (my $attr_index = 0; $attr_index < $ac; ++$attr_index) {
        $get = $get . convertAndAddToPythonDictionaryObject($ituple.'.get_'.$name.'()[list_index]', $attr_index, $attrTypes[$attr_index], $attrNames[$attr_index], 'pyNamesNestedTuple', 'pyDict1', $spaces."        ", $output_dir);
      }    
      $get = $get . $spaces."        value = pyDict1;\n";
      $get = $get . $spaces."      }\n";
      $get = $get . $spaces."      PyList_SET_ITEM(pyList, list_index, value);\n";
      $get = $get . $spaces."    }\n";
      $get = $get . $spaces."    value = pyList;\n";
      $get = $get . $spaces."  }\n";
    }
  }
  ########### SPL: tuple of tuple --> PyDict of PyDict #########################
  if (SPL::CodeGen::Type::isTuple($type)) {
    $nested_tuple = 1;
    $get = "". $spaces."{\n";
    $get = $get . $spaces."  // SPL: tuple of tuple --> PyDict of PyDict\n";
    $get = $get . $spaces."  // $type $name\n";
    $get = $get . $spaces."  PyObject * value = 0;\n";
    $get = $get . $spaces."  {\n";
    $get = $get . $spaces."    PyObject * pyDict1 = PyDict_New();\n";
      my $ac = 0;
      for my $_attrName (SPL::CodeGen::Type::getAttributeNames ($type)) {  	
        $ac++;
      }
      $get = $get . $spaces."    PyObject * pyNamesNestedTuple = PyTuple_New(".$ac.");\n";
      my $j = 0;
      for my $_attrName (SPL::CodeGen::Type::getAttributeNames ($type)) {   
      	$get = $get . $spaces."    PyObject * pyName".$j." = pyUnicode_FromUTF8(\"".$_attrName."\");\n"; 
        $get = $get . $spaces."    PyTuple_SET_ITEM(pyNamesNestedTuple, ".$j.", pyName".$j.");\n";
        $j++;
      }
      my @attrTypes = SPL::CodeGen::Type::getAttributeTypes ($type);
      my @attrNames = SPL::CodeGen::Type::getAttributeNames ($type); 
      for (my $attr_index = 0; $attr_index < $ac; ++$attr_index) {
        $get = $get . convertAndAddToPythonDictionaryObject($ituple.'.get_'.$name.'()', $attr_index, $attrTypes[$attr_index], $attrNames[$attr_index], 'pyNamesNestedTuple', 'pyDict1', $spaces."    ", $output_dir);
      }
      $get = $get . $spaces."    value = pyDict1;\n";
      $get = $get . $spaces."  }\n";
  }
  ###########################################################################
  if ($nested_tuple == 0){
    $get = _attr2Value($ituple, $type, $name, $spaces);
  }

  # PyTuple_GET_ITEM returns a borrowed reference.
  $getkey = $spaces.'  PyObject * key = PyTuple_GET_ITEM(' . $names . ',' . $i . ");\n";

  # Note PyDict_SetItem does not steal the references to the key and value
  my $setdict =  $spaces."  PyDict_SetItem(".$dictname.", key, value);\n";
  $setdict =  $setdict . $spaces."  Py_DECREF(value);\n";

  return $get . $getkey . $setdict . $spaces."}\n" ;
}

#
# Convert attribute of an SPL tuple to Python
# and add to a tuple object.
#
# ituple - C++ expression of the tuple
# i  - Attribute index
# type - spl type
# name - attribute name
# names - PyObject * pointing to Python tuple containing attribute names.

sub convertAndAddToPythonTupleObject {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];
  my $spaces = $_[4];
  my $output_dir = $_[5];

  # starts a C++ block and sets value
  my $get;
  my $nested_tuple = 0;
  ########### SPL: Map of tuple #########################
  if (SPL::CodeGen::Type::isMap($type)) {  
    if (SPL::CodeGen::Type::isTuple(SPL::CodeGen::Type::getValueType($type))) {
      $nested_tuple = 1;
      $get = "". $spaces."{\n";
      $get = $get . $spaces."  // SPL: Map with tuple as value type\n";
      $get = $get . $spaces."  // key type: ".SPL::CodeGen::Type::getKeyType($type)."\n";    
      $get = $get . $spaces."  // value type: ".SPL::CodeGen::Type::getValueType($type)."\n";
      $get = $get . $spaces."  PyObject * value = 0;\n";
    } elsif (SPL::CodeGen::Type::isList(SPL::CodeGen::Type::getValueType($type))) {
      my $element_type = SPL::CodeGen::Type::getElementType(SPL::CodeGen::Type::getValueType($type));  
      if (SPL::CodeGen::Type::isTuple($element_type)) {
      	$nested_tuple = 1;
      $get = "". $spaces."{\n";
      $get = $get . $spaces."  // SPL: Map with list of tuple as value type\n";
      $get = $get . $spaces."  // key type: ".SPL::CodeGen::Type::getKeyType($type)."\n";    
      $get = $get . $spaces."  // value type: ".SPL::CodeGen::Type::getValueType($type)."\n";
      $get = $get . $spaces."  PyObject * value = 0;\n";
      }
    }
  }  
  ########### SPL: list of tuple #########################
  if (SPL::CodeGen::Type::isList($type)) {
    my $element_type = SPL::CodeGen::Type::getElementType($type);  
    if (SPL::CodeGen::Type::isTuple($element_type)) {
      $nested_tuple = 1;
      $get = "". $spaces."{\n";
      $get = $get . $spaces."  // SPL: List of tuple\n";
      $get = $get . $spaces."  // element type: ".$element_type."\n";
      $get = $get . $spaces."  PyObject * value = 0;\n";
    }
  }
  ########### SPL: tuple of tuple #########################
  if (SPL::CodeGen::Type::isTuple($type)) {
    $nested_tuple = 1;
    $get = "". $spaces."{\n";
    $get = $get . $spaces."  // SPL: tuple of tuple\n";
    $get = $get . $spaces."  // $type $name\n";
    $get = $get . $spaces."  PyObject * value = 0;\n";
    $get = $get . $spaces."  {\n";
    my $ac = 0;
    for my $_attrName (SPL::CodeGen::Type::getAttributeNames ($type)) {  	
      $ac++;
    }
    $get = $get . $spaces."    PyObject * pyTuple = PyTuple_New($ac);\n";
    my @attrTypes = SPL::CodeGen::Type::getAttributeTypes ($type);
    my @attrNames = SPL::CodeGen::Type::getAttributeNames ($type); 
    for (my $attr_index = 0; $attr_index < $ac; ++$attr_index) {
      $get = $get . convertAndAddToPythonTupleObject($ituple.'.get_'.$name.'()', $attr_index, $attrTypes[$attr_index], $attrNames[$attr_index], $spaces."    ", $output_dir);
    }
    $get = $get . $spaces."  }\n";
    $get = $get . $spaces."  value = pyTuple;\n";
  }
  ###########################################################################
  if ($nested_tuple == 0){    
    $get = _attr2Value($ituple, $type, $name, $spaces);
  }
  my $settuple =  $spaces."  PyTuple_SET_ITEM(pyTuple, $i, value);\n";

  return $get . $settuple . $spaces. "}\n" ;
}

# opens the xml file for the type header file passed as $type_file for reading
# does string parsing line by line and checks if $attr and $cpptype and $spltype are present in this file
# returns 1 if attribute, cpptype and spltype are found in xml file, otherwise 0
sub is_type_in_file {
  my $attr = $_[0]; # name of the tuple attribute
  my $cpptype = $_[1]; # type to search for
  my $spltype = $_[2]; # type to search for
  my $type_dir = $_[3]; # ../src/type
  my $type_file = $_[4];
  
  my $result = 0;
  my $filepath = $type_dir."/".substr($type_file, 0, -2).".xml";
  open my $xmlfile, $filepath or die "The type xml file can not be opened: ".$filepath; 
  my $attrFound = 0;
  my $cppTypeFound = 0;
  my $splTypeFound = 0;
  $spltype =~ s/</&lt;/g;
  $spltype =~ s/>/&gt;/g;
  while( my $line = <$xmlfile>)  {   
    if ($attrFound == 0) {
      if ($line =~ m/.*>$attr<.*/) {
        $attrFound = 1;
      }
    }
    if ($attrFound == 1) {
      if (($line =~ m/.*lt;$cpptype&gt;.*/) or ($line =~ m/.*, $cpptype&gt;.*/) or ($line =~ m/.*SPL:list&lt;$cpptype&gt;.*/)) {
        $cppTypeFound = 1;
      }
    }
    if ($cppTypeFound == 1) {
      if ($line =~ m/.*$spltype.*/) {
        close $xmlfile;
        return 1;
      }
    }
  }  
  close $xmlfile;
  return $result;
}
sub spl_cpp_type {
  my $attr = $_[0]; # name of the tuple attribute to search for (attribute of type SPL:list or SPL:map with tuple as element type or value type)
  my $type = $_[1]; # spl type prefix to search for, e.g. SPL::list or SPL::map
  my $spltype = $_[2]; # embedded/nested tuple type to search for as spl type: e.g. tuple<float64 start_time,float64 end_time,float64 confidence>, provide value type of spl.map or element type of spl.list
  my $output_dir = $_[3]; # $model->getContext()->getOutputDirectory()

  my $resultType = "";
  my $cppType;
  my $dir = $output_dir . "/src/type";
  opendir(TYPEDIR, $dir);
  # we want to know the header files in which we search for type definitions
  @files = grep(/\.h$/,readdir(TYPEDIR));
  closedir(TYPEDIR);

  foreach $file (@files) {
    # Read the file content of the header file, grep for type definitions only.
    my $filepath = $dir."/".$file;
    open FILE, $filepath or die "The type header file can not be opened: ".$filepath;
    my @content = grep { m/ typedef .*_type;/ } <FILE>;
    chomp @content;
    close FILE;

    # type definition in header is like this: typedef SPL::list<SPL::cppType > spotted_type;
    # parameters are and result variables:           |$type    |$resultType"  |$attr  |
    #                                                 input     output         input
    foreach (@content) {
      if (m/^\s*typedef\s+(.+)\s+(\w+)_type;\s*$/) {
        if ((index($1, $type) != -1) && (index($2, $attr) != -1)) {
          $cppType = $1;
          $cppType =~ s/^.+:://;
          $cppType =~ s/\>//;
          $cppType =~ s/ >//g;
          # the cppType found in headers can exist multiple times, for example same attribute but different tuple types
          # therefore we check the cpp type with the corresponding spltype in xml file that belongs to the header file in type directory
          if (is_type_in_file($attr, "SPL::".$cppType, $spltype, $dir, $file)) {
            $resultType = "SPL::".$cppType;
          }
        }
      }
    }
  }
  return $resultType;
}

## Execute pip to install packages in the
## applications output directory under
## /etc/streamsx.topology/python
## They will be added to Python's path.
## Picks up pip from the path.
##
sub spl_pip_packages {

  my $model = $_[1];
  my $packages = $_[2];
  my $reqFile = $model->getContext()->getToolkitDirectory()."/opt/python/streams/requirements.txt";

  my $needPip = @$packages || -r $reqFile;

  if ($needPip) {

    my $pip = $_[0] == 3 ? 'pip3' : 'pip';
    use File::Path qw(make_path);
    my $pkgDir = $model->getContext()->getOutputDirectory()."/etc/streamsx.topology/python";
    make_path($pkgDir);

# Need pip 9
# '--upgrade-strategy', 'only-if-needed');


    my $rcv2 = `$pip --version`;
    if ( ! defined $rcv2) {
        # If pip3 doesn't exist use pip
        if ($pip == 'pip3') {
            $pip = 'pip';
            $rcv2 = `$pip --version`;
        }
    }
    SPL::CodeGen::println("pip version:" . $rcv2);

    my @pipCmd = ($pip, 'install', '--disable-pip-version-check', '--user', '--upgrade');


    print("#if 0\n");
    print("/*\n");

    my $pub = $ENV{'PYTHONUSERBASE'};
    $ENV{'PYTHONUSERBASE'} = $pkgDir;

    if (-r $reqFile) {
      SPL::CodeGen::println("Installing Python packages from requirements:");
      push(@pipCmd, ('--requirement', $reqFile));
      SPL::CodeGen::println("Executing pip:" . join(' ', @pipCmd));
      my $rc = system(@pipCmd);
      if ($rc != 0) {
        SPL::CodeGen::errorln("pip failed for requirements" . join(' ', @pipCmd));
      } 
      pop(@pipCmd);
      pop(@pipCmd);
    }

    if (@$packages) {
      SPL::CodeGen::println("Installing Python packages:" . join(' ', @$packages));
      push(@pipCmd, @$packages);
      SPL::CodeGen::println("Executing pip:" . join(' ', @pipCmd));
      my $rc = system(@pipCmd);
      if ($rc != 0) {
        SPL::CodeGen::errorln("pip failed for packages:" . join(' ', @pipCmd));
      } 
    }

    print("*/\n");
    print("#endif\n");

    if ($pub) {
      $ENV{'PYTHONUSERBASE'} = $pub;
    }
  }
}

my $model;  # local copy of the operator model variable

#
# Initialize this module.
#
sub splpyInit {
    if (defined $model) {
        # Attempt to detect possible future concurrent processing of operators.
        SPL::CodeGen::errorln("Internal error: splpyInit() already called.");
    }
    ($model) = @_;
}

#
# Return true if optional data types are supported, else false.
#
sub hasOptionalTypesSupport {
    return hasMinimumProductVersion("4.3");
}

#
# Return true if the Streams product version matches or exceeds
# the given version number in "VRMF" format, else false.
#
# Note: This test assumes the fixpack ("F") is numeric, or not specified.
#
sub hasMinimumProductVersion {
    my ($requiredVersion) = @_;

    my @vrmf = split(/\./, $requiredVersion);
    my @pvrmf = split(/\./, $model->getContext()->getProductVersion());
    for (my $i = 0; $i <= $#vrmf; $i++) {
        if (!($vrmf[$i] =~ /^\d+$/)) {
            SPL::CodeGen::errorln("Invalid version: " . $requiredVersion);
            return 0;
        }
        return 0 if ($i > $#pvrmf);
        return 0 if ($pvrmf[$i] < $vrmf[$i]);
        return 1 if ($pvrmf[$i] > $vrmf[$i]);
    }
    return 1;
}

1;
