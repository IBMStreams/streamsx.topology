/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.tuple;

import java.io.Serializable;

/**
 * A tuple that has a partition key.
 * 
 * @param <K>
 *            Type of the key.
 * 
 * @see com.ibm.streamsx.topology.TStream#parallel(int)
 * @see com.ibm.streamsx.topology.TWindow
 */
public interface Keyable<K> extends Serializable {

    /**
     * Returns the partition key for this tuple.
     * 
     * @return the partition key for this tuple
     */
    K getKey();
}
