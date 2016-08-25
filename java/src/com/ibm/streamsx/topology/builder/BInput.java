/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.builder;

public class BInput extends BJSONObject {

    private final GraphBuilder builder;

    protected BInput(GraphBuilder builder) {
        this.builder = builder;
    }

    public GraphBuilder builder() {
        return builder;
    }
}
