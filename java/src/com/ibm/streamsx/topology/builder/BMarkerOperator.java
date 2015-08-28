/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

public class BMarkerOperator extends BOperator {

    public BMarkerOperator(GraphBuilder bt, BVirtualMarker virtualMarker) {
        super(bt);
        json().put("kind", virtualMarker.kind());
        json().put("marker", true);
    }
}
