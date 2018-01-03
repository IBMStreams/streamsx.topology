# Shared aspects of operators

## Fields

* `funcop_` - Maintains the life-cycle object that holds the Python callable.
* `pyInStyleObj_` field : Operators with input ports have an instance field that
  optionally holds a Python object for aiding in handling input tuples.
  The specific value is based upon the style:
  * `dict` - Attribute names for input port 0 as a tuple. Defined as `pyInNames_`
  Defines are in `pyspltuple.cgt` to provide more readable code indicating the actual use of the value.

## Shared cgt files

* `pyspltuple.cgt` - Setup for operators processing tuples, sets some perl variables.
* `pyspltuple_constructor.cgt` - Common constructor code for operators processing tuples.
* `pyspltuple2value.cgt` - Common process method code for for operators processing tuples. Results in a C++ variable `value` set to the Python object to be passed into Python callable.
* `pyspltuple2dict.cgt` - Convert an SPL tuple to a Python dict object.
* `pyspltuple2tuple.cgt` - Convert an SPL tuple to a Python tuple object.
