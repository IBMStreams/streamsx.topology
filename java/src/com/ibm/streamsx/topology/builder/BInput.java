/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

public class BInput extends BJSONObject implements BPort {

    private final GraphBuilder builder;

    protected BInput(GraphBuilder builder) {
        this.builder = builder;
    }

    public GraphBuilder builder() {
        return builder;
    }
}
