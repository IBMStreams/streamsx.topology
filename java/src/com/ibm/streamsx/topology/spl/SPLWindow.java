/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TWindow;

/**
 * An SPL window, that has an SPL eviction policy and an SPL trigger policy.
 */
public interface SPLWindow extends TWindow<Tuple,Object>, SPLInput {

    /**
     * SPL stream for this window.
     * 
     * @return SPL stream for this window.
     */
    @Override
    public SPLStream getStream();
}
