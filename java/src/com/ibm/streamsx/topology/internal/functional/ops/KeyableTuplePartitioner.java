/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.tuple.Keyable;

@PrimitiveOperator
@Icons(location16 = "opt/icons/functor_16.gif", location32 = "opt/icons/functor_32.gif")
public class KeyableTuplePartitioner extends HashAdder {

    @Override
    int getHash(Object value) {
        Keyable<?> keyable = (Keyable<?>) value;
        return keyable.getKey().hashCode();
    }
}
