/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import com.ibm.streamsx.topology.TopologyElement;

public abstract class TupleContainer<T> extends TopologyItem {

    private final Class<T> tupleClass;

    // private final Set<Stream.Policy> policies = new HashSet<Stream.Policy>();

    protected TupleContainer(TopologyElement fe, Class<T> tupleClass) {
        super(fe);
        this.tupleClass = tupleClass;
    }

    public final Class<T> getTupleClass() {
        return tupleClass;
    }

    /*
     * protected Set<Stream.Policy> policies() { return policies; }
     */
}
