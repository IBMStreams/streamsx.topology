from streamsx.topology.topology import Topology
import streamsx.topology.context
import transform_sample_functions;

def main():
    topo = Topology("transform_sample")
    source = topo.source(transform_sample_functions.int_strings_transform)
    i1 = source.transform(transform_sample_functions.string_to_int)
    i2 = i1.transform(transform_sample_functions.AddNum(17))
    i2.print()
    streamsx.topology.context.submit("DISTRIBUTED", topo.graph)

if __name__ == '__main__':
    main()
