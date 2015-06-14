/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

public class BMarkerOperator extends BOperator {

    public BMarkerOperator(GraphBuilder bt, String kind) {
        super(bt);
        json().put("kind", kind);
        json().put("marker", true);
    }
}
