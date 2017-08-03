/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_VIRTUAL;

public class BMarkerOperator extends BOperator {

    public BMarkerOperator(GraphBuilder bt, BVirtualMarker virtualMarker) {
        super(bt);
        _json().addProperty(KIND, virtualMarker.kind());
        _json().addProperty("marker", true);
        _json().addProperty(MODEL, MODEL_VIRTUAL);
    }
}
