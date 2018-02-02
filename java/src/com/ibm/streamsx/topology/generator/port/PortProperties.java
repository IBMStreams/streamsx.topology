package com.ibm.streamsx.topology.generator.port;

public interface PortProperties {
	/**
	 * The width of the parallel region, if the port is the start of a parallel region.
	 */
	String WIDTH = "width";
	
	/**
	 * Boolean top-level parameter indicating whether the port should broadcast tuples
	 * to all channels in a parallel region, if the port is the start of a parallel
	 * region.	
	 */
	String BROADCAST = "broadcast";

	
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
