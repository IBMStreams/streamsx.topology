###########################
Restrictions and known bugs
###########################

* For Python development outside of a CP4D, for example with a Jupyter notebook outside of CP4D, you must use an Anaconda or Miniconda Python installation.
* No support for nested parallel regions at sources, i.e. nested :py:func:`streamsx.topology.topology.Stream.set_parallel`, for example::

    topo = Topology()
    s = topo.source(S())
    s.set_parallel(3).set_parallel(2)

  In this example, `set_parallel(3)` is ignored.

* When tuples are nested within other tuples in a stream schema, the call style of instances of `StreamSchema` for a callable is always dict, whatever value the `style` property has. When the return value of a callable represents also a structured schema with nested tuples, the return type must also be a `dict`. Otherwise the behaviour is not defined.

* No schema support for container types (list, map, set, and the like) with non-primitive value or element types as value or element types for other containers, also when encapsulated in a named tuple::

    class A_schema(typing.NamedTuple):
        x: int
        y: int

    class B_schema(typing.NamedTuple):
        a_list: typing.List[A_schema]     # supported, A_schema does not contain a container type at all

    class C_schema(typing.NamedTuple):
        c1: str
        c2: B_schema                      # supported, a container type can be nested at any depth

    class D_schema(typing.NamedTuple):
        d1: str
        d2: typing.Mapping[int, typing.List[int]       # supported
        d3: typing.Mapping[int, typing.List[A_schema]  # not supported: a container with non-primitive element type is direct value type of a map

    class E_schema(typing.NamedTuple):
        e1: bool
        e2: typing.Mapping[str, C_schema]   # not supported: C_schema.c2.a_list is a list with non-primitive element type

* Schemas support only primitive types for the key type of a map::

    class A_schema(typing.NamedTuple):
        a1: int
        a2: int

    class B_schema(typing.NamedTuple):
        b1: str
        b2: typing.Mapping[str, A_schema]   # supported
        b3: typing.Mapping[A_schema, str]   # not supported, A_schema not supported as key type

* Schemas support only primitive types as element type of a set::

    class A_schema(typing.NamedTuple):
        a1: int
        a2: int

    class B_schema(typing.NamedTuple):
        b1: int
        b2: typing.Set[int]       # supported
        b3: typing.Set[A_schema]  # not supported

* Python Composites (derived from :py:class:`streamsx.topology.composite.Composite`) can have only one input port.
* No support to process final marker (end of stream) in Python Callables like in SPL operators
* No hook for drain processing in consistent region for Python Callables
* Submission time parameters, which are defined in SPL composites of other toolkits, or created by using
  `streamsx.spl.op.Expression` in the topology, cannot be accessed at runtime with `streamsx.ec.get_submission_time_value(name)`.
