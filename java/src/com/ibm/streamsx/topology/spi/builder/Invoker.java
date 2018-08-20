/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.spi.builder;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.logic.ObjectUtils.serializeLogic;
import static com.ibm.streamsx.topology.spi.builder.Utils.copyParameters;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

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
     * @param kind Java functional operator kind.
     * @param invokeInfo Operator invocation information.
     * @param logic Functional logic.
     * @param tupleType Type of tuples for the returned stream.
     * @param outputSerializer How output tuples are serialized.
     * @param parameters Additional SPL operator parameters. 
     * 
     * @return Stream produced by the source operator invocation.
     */
    static <T> TStream<T> invokeSource(Topology topology, String kind, JsonObject invokeInfo,
            Supplier<Iterable<T>> logic, Type tupleType, TupleSerializer outputSerializer,
            Map<String, Object> parameters) {
        
        parameters = copyParameters(parameters);

        if (outputSerializer != null) {
            parameters.put("outputSerializer", serializeLogic(outputSerializer));
        }

        BOperatorInvocation source = JavaFunctional.addFunctionalOperator(topology,
                jstring(invokeInfo, "name"),
                kind,
                logic, parameters);

        // Extract any source location information from the config.
        SourceInfo.setInvocationInfo(source, invokeInfo);

        return JavaFunctional.addJavaOutput(topology, source, tupleType, ofNullable(outputSerializer), true);
    }
    
    /**
     * Invoke a functional for each operator consuming a single stream.
     * @param stream Stream to be consumed.
     * @param kind Java functional operator kind.
     * @param invokeInfo Operator invocation information.
     * @param logic Functional logic.
     * @param tupleSerializer How tuples are serialized.
     * @param parameters Additional SPL operator parameters.
     * @return Sink for the terminating logic.
     */
    static <T> TSink invokeForEach(
            TStream<T> stream,
            String kind,
            JsonObject invokeInfo,
            Consumer<T> logic,
            TupleSerializer tupleSerializer,
            Map<String,Object> parameters) {
        
        parameters = copyParameters(parameters);
        
        if (tupleSerializer != null)          
            parameters.put("inputSerializer", serializeLogic(tupleSerializer));
        
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
     * @param kind Java functional operator kind.
     * @param stream Single input stream.
     * @param invokeInfo Operator invocation information.
     * @param logic Logic performed against each tuple.
     * @param tupleType Type of tuples for the returned stream.
     * @param inputSerializer How input tuples are serialized.
     * @param outputSerializer How output tuples are serialized.
     * 
     * @return Stream produced by the pipe operator.
     */
    static <T,R> TStream<?> invokePipe(
            String kind,
            TStream<T> stream,           
            JsonObject invokeInfo,         
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
                jstring(invokeInfo, "name"),
                kind,
                logic, parameters);
        
        if (inputSerializer != null) {
            JavaFunctional.addDependency(stream, pipe, inputSerializer.getClass());
        }

        // Extract any source location information from the config.
        SourceInfo.setInvocationInfo(pipe, invokeInfo);
        
        stream.connectTo(pipe, true, null);
        
        return JavaFunctional.addJavaOutput(stream, pipe,
                tupleType, ofNullable(outputSerializer), true);
    }
    
    /**
     * Invoke a functional operator consuming an arbitrary number of
     * input streams and producing an arbitrary number of output streams.
     * 
     * @param te Topology element.
     * @param kind Java functional operator kind.
     * @param streams Input streams.
     * @param invokeInfo Operator invocation information.
     * @param logic Logic to invoke for each input tuple.
     * @param tupleTypes Tuple types for the output stream.
     * @param inputSerializers How input tuples are serialized.
     * @param outputSerializers How output tuples are serialized.
     * @param parameters Parameters for the operator invocation.
     * 
     * @return Streams produced by the primitive operator.
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
            
            assert streams.size() == inputSerializers.size();
                      
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
            
            assert tupleTypes.size() == outputSerializers.size();
            
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
        
        if (inputSerializers != null) {
            for (TupleSerializer serializer : inputSerializers)
                if (serializer != null)
                    JavaFunctional.addDependency(te, primitive, serializer.getClass());
        }

        // Extract any source location information from the config.
        SourceInfo.setInvocationInfo(primitive, invokeInfo);
        
        for (TStream<?> stream : streams)
            stream.connectTo(primitive, true, null);
        
        List<TStream<?>> outputs = null;
        
        if (tupleTypes != null) {       
            outputs = new ArrayList<>(tupleTypes.size());
            for (int i = 0; i < tupleTypes.size(); i++) {
                    Type tupleType = tupleTypes.get(i);
                outputs.add(JavaFunctional.addJavaOutput(te, primitive, tupleType,
                        outputSerializers == null ? empty() : ofNullable(outputSerializers.get(i)),
                        tupleTypes.size() == 1));
            }
        }
           
        return outputs;
    }
    
    /**
     * Set the functional namespace for Java functional operators
     * for a topology.
     * 
     * This is used to ensure the topologies declared using an SPI toolkit
     * invoke its own versions of the operators from this topology toolkit.
     * For example HashAdder, HashRemover etc. This means:
     *   * The operators are in-sync with the topology definition
     *   * The share the same class loader as any other operators
     *   provided by the toolkit using the SPI.
     *   
     * This must be called immediately after the Topology is created.
     */
    static void setFunctionalNamespace(Topology topology, String namespace) {
        topology.builder().setFunctionalNamespace(namespace);
    }
}
