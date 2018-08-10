/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.lang.reflect.Type;

public class BOutputPort extends BOutput {

    private final BOperatorInvocation op;

    BOutputPort(BOperatorInvocation op, int index, String name, String schema) {
        this.op = op;
        addPortInfo(index, name, schema);
    }

    public BOperatorInvocation operator() {
        return op;
    }

    public GraphBuilder builder() {
        return op.builder();
    }

    @Override
    public void connectTo(BInputPort input) {
        connect(input);
        input.connect(this);
               
        input.operator().copyRegions(operator());
    }
    @Override
    public String _type() {
        return _schema();
    }
    
    public void setNativeType(Type tupleType) {
        _json().addProperty("type.native", tupleType.getTypeName());
    }
}
