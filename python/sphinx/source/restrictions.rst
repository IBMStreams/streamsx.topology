###########################
Restrictions and known bugs
###########################

* No support for nested parallel regions at sources, i.e. nested :py:func:`streamsx.topology.topology.Stream.set_parallel`, for example::

    topo = Topology()
    s = topo.source(S())
    s.set_parallel(3).set_parallel(2)

  In this example, `set_parallel(3)` is ignored.

* No support for nested types when defining stream schemas, for example::

    class NamedTupleNestedTupleSchema(typing.NamedTuple):
        key: str
        spotted: SpottedSchema

* No support of collections of `NamedTuple` as stream schema, for example::

    class NamedTupleListOfTupleSchema(typing.NamedTuple):
        spotted: typing.List[SpottedSchema]

* Python Composites (derived from :py:class:`streamsx.topology.composite.Composite`) can have only one input port.
* No support to process window markers or final marker (end of stream) in Python Callables like in SPL operators
* No hook for drain processing in consistent region for Python Callables
* Submission time parameters, which are defined in SPL composites of other toolkits, or created by using
  `streamsx.spl.op.Expression` in the topology, cannot be accessed at runtime with `streamsx.ec.get_submission_time_value(name)`.
