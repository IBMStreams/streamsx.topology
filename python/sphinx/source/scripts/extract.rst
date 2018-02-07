##################
spl-python-extract
##################

********
Overview
********

Extracts SPL Python primitive operators from decorated
Python classes and functions.

Executing this script against an SPL toolkit creates the SPL
primitive operator meta-data required by the SPL compiler (`sc`).

*****
Usage
*****

::

    spl-python-extract [-h] -i DIRECTORY [--make-toolkit] [-v]

    Extract SPL operators from decorated Python classes and functions.

    optional arguments:
      -h, --help            show this help message and exit
      -i DIRECTORY, --directory DIRECTORY
                            Toolkit directory
      --make-toolkit        Index toolkit using spl-make-toolkit
      -v, --verbose         Print more diagnostics

******************************
SPL Python primitive operators
******************************

SPL operators that call a Python function or callable class are created by
decorators provided by the `streamsx` package.

``spl-python-extract`` is a Python script that creates SPL operators from
Python functions and classes contained in modules in a toolkit.
The resulting operators embed the Python runtime to allow stream
processing using Python.

To create SPL operators from Python functions or classes one or more Python
modules are created in the ``opt/python/streams`` directory
of an SPL toolkit.

Each module must import the ``streamsx.spl``
package which is located in the `opt/packages` directory of this toolkit.
It contains the decorators use to create SPL operators from Python functions.
The module must also define a function `spl_namespace` that returns a string
containing the SPL namespace the operators for that module will be placed in.

For example::

    # Import the SPL decorators
    from streamsx.spl import spl
    
    # Defines the SPL namespace for any functions in this module
    # Multiple modules can map to the same namespace
    def spl_namespace():
       return "com.ibm.streamsx.topology.pysamples.mail"

Decorating a Python class produces a stateful SPL operator. The instance fields of the class are the state for the operator. Any parameters to the
``__init__`` method (excluding ``self``) are mapped to
SPL operator parameters.

Decorating a Python function produces a stateless SPL operator. The function may reference variables in the module that are effectively state but such variables are shared by all invocations of the operator within the same processing element.

Any Python docstring for the function or class is copied into the SPL operator's description field in its operator model, providing a description for IDE developers using the toolkit.

Functions or classes in the modules that are not decorated, decorated with ``@spl.ignore`` or start with ``spl`` are ignored and will not result in any SPL operator.

Python classes as SPL operators
===============================

Decorating a Python class creates a stateful SPL operator
where the instance fields of the class are the operator's state. An instance
of the class is created when the SPL operator invocation is initialized
at SPL runtime. The instance of the Python class is private to the SPL
operator and is maintained for the lifetime of the operator.

If the class has instance fields then they are the state of the
operator and are private to each invocation of the operator.

If the ``__init__`` method has parameters beyond the first
``self`` parameter then they are mapped to operator parameters.
Any parameter that has a default value becomes an optional parameter
to the SPL operator. Parameters of the form ``\*args`` and ``\*\*kwargs``
are not supported.

The value of the operator parameters at SPL operator invocation are passed
to the ``__init__`` method. This is equivalent to creating an instance
of the class passing the operator parameters into the constructor.

For example, with this decorated class producing an SPL source
operator::

    @spl.source()
    class Range:
        def __init__(self, stop, start=0):
            self.start = start
            self.stop = stop

        def __iter__(self):
            return zip(range(self.start, self.stop))

The SPL operator ``Range`` has two parameters, ``stop`` is mandatory and ``start`` is optional, defaulting to zero. Thus the SPL operator may be invoked as::

    // Produces the sequence of values from 0 to 99
    //
    // Creates an instance of the Python class
    // Range using Range(100)
    //
    stream<int32 seq> R = Range() {
      param
        stop: 100;
    }

or both operator parameters can be set::

    // Produces the sequence of values from 50 to 74
    //
    // Creates an instance of the Python class
    // Range using Range(75, 50)
    //
    stream<int32 seq> R = Range() {
      param
        start: 50;
        stop: 75;
    }

Python functions as SPL operators
=================================

Decorating a Python function creates a stateless SPL operator.
In SPL terms this is similar to an SPL Custom operator, where
the code in the Python function is the custom code. For
operators with input ports the function is called for each
input tuple, passing a Python representation of the SPL input tuple.
For a SPL source operator the function is called to obtain an iterable
whose contents will be submitted to the output stream as SPL tuples.

Operator parameters are not supported.

An example SPL sink operator that prints each input SPL tuple after
its conversion to a Python tuple::

    @spl.for_each()
    def PrintTuple(*tuple):
        """Print each tuple to standard out."""
         print(tuple, flush=True)


