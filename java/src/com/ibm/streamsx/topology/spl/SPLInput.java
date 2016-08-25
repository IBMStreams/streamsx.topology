/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.spl;

import com.ibm.streamsx.topology.TopologyElement;

/**
 * Interface that can be connected to an SPL input port.
 *
 */
public interface SPLInput extends TopologyElement {
    SPLStream getStream();
}
