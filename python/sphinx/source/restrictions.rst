###########################
Restrictions and known bugs
###########################

* No support for nested parallel regions at sources, i.e. nested :py:func:`streamsx.topology.topology.Stream.set_parallel`, for example::

    topo = Topology()
    s = topo.source(S())
    s.set_parallel(3).set_parallel(2)

  In this example, `set_parallel(3)` is ignored.

* No support for nested types container types when defining stream schemas, for example a map with a tuple as value type is supported, but not if this is part of another map as value type::

    #tuple<int64 x_coord, int64 y_coord>
    class Point2DSchema(typing.NamedTuple):
        x_coord: int
        y_coord: int

    #tuple<int64 int1, map<string, tuple<int64 x_coord, int64 y_coord>> map1>
    class TupleWithMapToTupleAttr1(typing.NamedTuple):
        int1: int
        map1: typing.Mapping[str,Point2DSchema] # supported
    
    #tuple<int64 int2, map<string, tuple<int64 int1, map<rstring, tuple<int64 x_coord, int64 y_coord>> map1>> map2>
    class TupleWithMapToTupleWithMap(typing.NamedTuple):
        int2: int
        map2: typing.Mapping[str,TupleWithMapToTupleAttr1] # not supported

* Python Composites (derived from :py:class:`streamsx.topology.composite.Composite`) can have only one input port.
* No support to process final marker (end of stream) in Python Callables like in SPL operators
* No hook for drain processing in consistent region for Python Callables
* Submission time parameters, which are defined in SPL composites of other toolkits, or created by using
  `streamsx.spl.op.Expression` in the topology, cannot be accessed at runtime with `streamsx.ec.get_submission_time_value(name)`.
