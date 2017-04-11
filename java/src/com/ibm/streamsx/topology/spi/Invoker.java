package com.ibm.streamsx.topology.spi;

import java.util.Map;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.spi.ops.Source;

/**
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 */
public interface Invoker {
    
    /**
     * Invoke a functional source operator that generates a single stream.
     * 
     * @param topology Topology the operator will be invoked in.
     * @param opClass Java functional operator class.
     * @param config Operator configuration.
     * @param logic Functional logic.
     * @param parameters Additional SPL operator parameters, specific to 
     * 
     * @return Stream produced by the source operator invocation.
     */
    static <T> TStream<T> invokeSource(
            Topology topology,
            Class<? extends Source> opClass,
            JsonObject config,
            Supplier<Iterable<T>> logic,
            Class<T> tupleClass,
            TupleSerializer tupleSerializer,
            Map<String,Object> parameters) {
        
        return null;
    }
}
