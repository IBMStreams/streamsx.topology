package com.ibm.streamsx.topology.spi;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.JavaFunctional;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.TSinkImpl;
import com.ibm.streamsx.topology.spi.operators.ForEach;
import com.ibm.streamsx.topology.spi.operators.Source;

/**
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * The config object contains information about the invocation of an operator.
 * 
 * 
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
            Type tupleType,
            TupleSerializer tupleSerializer,
            Map<String,Object> parameters) {
        
        BOperatorInvocation source = JavaFunctional.addFunctionalOperator(topology,
                jstring(config, "name"),
                opClass, logic);
        
        // Extract any source location information from the config.
        SourceInfo.setSourceInfo(source, config);
        
        return JavaFunctional.addJavaOutput(topology, source, tupleType);
    }
    
    static <T> TSink invokeForEach(
            TStream<T> stream,
            Class<? extends ForEach> opClass,
            JsonObject config,
            Consumer<T> logic,
            TupleSerializer tupleSerializer,
            Map<String,Object> parameters) {
        
        BOperatorInvocation forEach = JavaFunctional.addFunctionalOperator(
                stream,
                jstring(config, "name"),
                opClass,
                logic);
        
        // Extract any source location information from the config.
        SourceInfo.setSourceInfo(forEach, config);

        stream.connectTo(forEach, true, null);
        
        return new TSinkImpl(stream, forEach);        
    }
}
