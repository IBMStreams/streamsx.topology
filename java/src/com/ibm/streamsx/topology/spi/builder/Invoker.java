/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.spi.builder;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.logic.ObjectUtils.serializeLogic;
import static com.ibm.streamsx.topology.spi.builder.Utils.copyParameters;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.ObjIntConsumer;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.JavaFunctional;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.TSinkImpl;
import com.ibm.streamsx.topology.spi.runtime.TupleSerializer;

/**
 * 
 * This is part of the SPI to allow additional functional style
 * functionality to built using the primitives provided by this toolkit.
 * 
 * The config object contains information about the invocation of an operator.
 * 
 * If executing in embedded then the JSON graph must be provided with
 * the mapping of Java primitive kind to Java class in {@link Properties.Graph.Config#JAVA_OPS}.
 */
public interface Invoker {
    
    /**
     * Invoke a functional source operator that generates a single stream.
     * 
     * @param topology Topology the operator will be invoked in.
     * @param opClass Java functional operator class.
     * @param invokeInfo Operator invocation information.
     * @param logic Functional logic.
     * @param parameters Additional SPL operator parameters. 
     * 
     * @return Stream produced by the source operator invocation.
     */
    static <T> TStream<T> invokeSource(Topology topology, String kind, JsonObject invokeInfo,
            Supplier<Iterable<T>> logic, Type tupleType, TupleSerializer outputSerializer,
            Map<String, Object> parameters) {
        
        parameters = copyParameters(parameters);

        if (outputSerializer != null)
            parameters.put("outputSerializer", serializeLogic(outputSerializer));

        BOperatorInvocation source = JavaFunctional.addFunctionalOperator(topology,
                jstring(invokeInfo, "name"),
                kind,
                logic, parameters);

        // Extract any source location information from the config.
        SourceInfo.setInvocationInfo(source, invokeInfo);

        return JavaFunctional.addJavaOutput(topology, source, tupleType);
    }
    
    /**
     * Invoke a functional for each operator consuming a single stream.
     * @param stream Stream to be consumed.
     * @param opClass Java functional operator class.
     * @param invokeInfo Operator invocation information.
     * @param logic Functional logic.
     * @param tupleSerializer How tuples are serialized.
     * @param parameters Additional SPL operator parameters.
     * @return
     */
    static <T> TSink invokeForEach(
            TStream<T> stream,
            String kind,
            JsonObject invokeInfo,
            Consumer<T> logic,
            TupleSerializer tupleSerializer,
            Map<String,Object> parameters) {
        
        BOperatorInvocation forEach = JavaFunctional.addFunctionalOperator(
                stream,
                jstring(invokeInfo, "name"),
                kind,
                logic,
                parameters);
        
        // Extract any source location information from the invokeInfo.
        SourceInfo.setInvocationInfo(forEach, invokeInfo);

        stream.connectTo(forEach, true, null);
        
        return new TSinkImpl(stream, forEach);        
    }
    
    /**
     * Invoke a functional map operator consuming a single
     * input stream and producing a single output stream.
     * 
     * @param streams
     * @param opClass
     * @param config Operator invocation information.
     * @param logic
     * @param tupleTypes
     * @param tupleSerializers
     * @return
     */
    static <T,R> TStream<?> invokePipe(
            String kind,
            TStream<T> stream,           
            JsonObject config,         
            Consumer<T> logic,
            Type tupleType,
            TupleSerializer inputSerializer,
            TupleSerializer outputSerializer,
            Map<String,Object> parameters
            ) {
        
        parameters = copyParameters(parameters);
        
        if (inputSerializer != null)          
            parameters.put("inputSerializer", serializeLogic(inputSerializer));
     
        if (outputSerializer != null)        
            parameters.put("outputSerializer", serializeLogic(outputSerializer));
        
        BOperatorInvocation pipe = JavaFunctional.addFunctionalOperator(stream,
                jstring(config, "name"),
                kind,
                logic, parameters);

        // Extract any source location information from the config.
        SourceInfo.setInvocationInfo(pipe, config);
        
        stream.connectTo(pipe, true, null);
        
        return JavaFunctional.addJavaOutput(stream, pipe, tupleType);
    }
    
    /**
     * Invoke a functional operator consuming an arbitrary number of
     * input streams and producing an arbitrary number of output streams.
     * 
     * @param streams
     * @param opClass
     * @param invokeInfo Operator invocation information.
     * @param logic
     * @param tupleTypes
     * @param tupleSerializers
     * @return
     */
    static List<TStream<?>> invokePrimitive(
            TopologyElement te,
            String kind,
            List<TStream<?>> streams,           
            JsonObject invokeInfo,         
            ObjIntConsumer<Object> logic,
            List<Type> tupleTypes,
            List<TupleSerializer> inputSerializers,
            List<TupleSerializer> outputSerializers,
            Map<String,Object> parameters
            ) {
        
        parameters = copyParameters(parameters);
        
        if (inputSerializers != null && !inputSerializers.isEmpty()) {
            String[] serializers = new String[inputSerializers.size()];
            for (int i = 0; i < serializers.length; i++) {
                TupleSerializer serializer = inputSerializers.get(i);
                if (serializer == null)
                    serializers[i] = "";
                else
                    serializers[i] = serializeLogic(serializer);
            }
            
            parameters.put("inputSerializer", serializers);
        }
        
        if (outputSerializers != null && !outputSerializers.isEmpty()) {
            String[] serializers = new String[outputSerializers.size()];
            for (int i = 0; i < serializers.length; i++) {
                TupleSerializer serializer = outputSerializers.get(i);
                if (serializer == null)
                    serializers[i] = "";
                else
                    serializers[i] = serializeLogic(serializer);
            }
            
            parameters.put("outputSerializer", serializers);
        }
        
        BOperatorInvocation primitive = JavaFunctional.addFunctionalOperator(te,
                jstring(invokeInfo, "name"),
                kind,
                logic, parameters);

        // Extract any source location information from the config.
        SourceInfo.setInvocationInfo(primitive, invokeInfo);
        
        for (TStream<?> stream : streams)
            stream.connectTo(primitive, true, null);
        
        List<TStream<?>> outputs = null;
        
        if (tupleTypes != null) {       
            outputs = new ArrayList<>(tupleTypes.size());
            for (Type tupleType : tupleTypes) {
                outputs.add(JavaFunctional.addJavaOutput(te, primitive, tupleType));
            }
        }
           
        return outputs;
    }
}
