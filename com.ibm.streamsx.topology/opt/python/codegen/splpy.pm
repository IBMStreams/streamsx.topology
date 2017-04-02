
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
        if (SPL::CodeGen::Type::isPrimitive($element_type)) {
            splToPythonConversionCheck($element_type);
            return;
        }
    }
    elsif (SPL::CodeGen::Type::isMap($type)) {
        my $key_type = SPL::CodeGen::Type::getKeyType($type);
        # Python maps must have hashable keys
        # (which excludes Python collection type such as list,map,set)
        # so for now restrict to primitive types)
        if (SPL::CodeGen::Type::isPrimitive($key_type)) {
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
    elsif(SPL::CodeGen::Type::isFloatingpoint($type)) {
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
    elsif (SPL::CodeGen::Type::isComplex32($type) || SPL::CodeGen::Type::isComplex64($type)) {
      return;
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

#
# Return a C++ statement converting a input attribute
# from an SPL input tuple to a Python object and
# setting it into pyTuple (as a Python Tuple).
# Assumes a C++ variable pyTuple are defined.
#
sub convertToPythonValueAsTuple {
  my $ituple = $_[0];
  my $i = $_[1];
  my $type = $_[2];
  my $name = $_[3];

  my $getAndConvert = convertAttributeToPythonValue($ituple, $type, $name);
  
  # Note PyTuple_SET_ITEM steals the reference to the value
  my $assign =  "    PyTuple_SET_ITEM(pyTuple, $i, $getAndConvert);\n";

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
  # instead cgt is used to generate code specific
  # the input schema
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
  $get = $get . convertAttributeToPythonValue($ituple, $type, $name);
  $get = $get . ";\n";

  # PyTuple_GET_ITEM returns a borrowed reference.
  $getkey = '{ PyObject * pyDictKey = PyTuple_GET_ITEM(' . $names . ',' . $i . ") ;\n";

# Note PyDict_SetItem does not steal the references to the key and value
  my $setdict =  "  PyDict_SetItem(pyDict, pyDictKey, pyValue);\n";
  $setdict =  $setdict . "  Py_DECREF(pyValue);}}\n";

  return $get . $getkey . $setdict ;
}

1;
