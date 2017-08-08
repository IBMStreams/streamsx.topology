/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_MARKER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_VIRTUAL;

class BMarkerOperator extends BOperator {

    BMarkerOperator(GraphBuilder bt, BVirtualMarker virtualMarker) {
        super(bt);
        _json().addProperty(KIND, virtualMarker.kind());
        _json().addProperty("marker", true);
        _json().addProperty(MODEL, MODEL_VIRTUAL);
        _json().addProperty(LANGUAGE, LANGUAGE_MARKER);
    }
}
