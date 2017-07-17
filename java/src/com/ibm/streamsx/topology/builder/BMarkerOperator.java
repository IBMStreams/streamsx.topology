/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_VIRTUAL;

import com.ibm.streamsx.topology.generator.operator.OpProperties;

public class BMarkerOperator extends BOperator {

    public BMarkerOperator(GraphBuilder bt, BVirtualMarker virtualMarker) {
        super(bt);
        json().put("kind", virtualMarker.kind());
        json().put("marker", true);
        json().put(MODEL, MODEL_VIRTUAL);
    }
}
