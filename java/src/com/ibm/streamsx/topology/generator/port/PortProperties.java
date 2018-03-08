package com.ibm.streamsx.topology.generator.port;

import com.ibm.streamsx.topology.generator.operator.OpProperties;

public interface PortProperties {
	/**
	 * The width of the parallel region, if the port is the start of a parallel region.
	 */
	String WIDTH = OpProperties.WIDTH;
	
	/**
	 * Routing style for a parallel output port.
	 */
	String ROUTING = "routing";
	
	/**
	 * Top-level boolean parameter indicating that the downstream parallel region
	 * is to be partitioned on the tuples of this port.
	 */
	String PARTITIONED = "partitioned";
	
	/**
	 * Top-level parameter indicating a list of keys on which the downstream
	 * parallel region is to be partitioned.
	 */
	String PARTITION_KEYS = "partitionedKeys";
}
