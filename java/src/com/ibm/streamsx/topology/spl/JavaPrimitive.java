/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND_CLASS;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.spl.OpAPIUtil.fixParameters;
import static com.ibm.streamsx.topology.spl.SPLStreamImpl.newSPLStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.ibm.streams.operator.Operator;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.model.Namespace;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.TSinkImpl;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Integration between Java topologies and SPL Java primitive operators.
 * <p>
 * In addition to the usual Java types used for operator parameter values,
 * a {@code Supplier<T>} parameter value may be specified.
 * Submission time parameters are passed in this manner.
 * See {@link Topology#createSubmissionParameter(String, Class)}.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aLong", topology.createSubmissionParameter(..., Long.class);
 * params.put("aShort", topology.createSubmissionParameter(..., (short)13);
 * ... = JavaPrimitive.invokeJavaPrimitive(..., params);
 * }</pre>
 */
public class JavaPrimitive {

    /**
     * Create an SPLStream from the invocation of an SPL Java primitive
     * operator with a single input port & output port.
     * The Java class {@code opClass} must be annotated with {@code PrimitiveOperator}.
     * 
     * @param opClass
     *            Class of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param outputSchema
     *            SPL schema of the operator's only output port.
     * @param params
     *            Parameters for the SPL Java Primitive operator, ignored if {@code null}.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeJavaPrimitive(
            Class<? extends Operator> opClass, SPLInput input,
            StreamSchema outputSchema, Map<String, ? extends Object> params) {

        BOperatorInvocation op = input.builder().addOperator(
                getInvocationName(opClass),
                getKind(opClass), fixParameters(params));
        op.setModel(MODEL_SPL, LANGUAGE_JAVA);
        op._json().addProperty(KIND_CLASS, opClass.getCanonicalName());
        SourceInfo.setSourceInfo(op, JavaPrimitive.class);
        SPL.connectInputToOperator(input, op);

        return newSPLStream(input, op, outputSchema, true);
    }
    
    /**
     * Invoke an SPL Java primitive operator with an arbitrary number
     * of input and output ports.
     * <BR>
     * Each input stream or window in {@code inputs} results in
     * a input port for the operator with the input port index
     * matching the position of the input in {@code inputs}.
     * If {@code inputs} is {@code null} or empty then the operator will not
     * have any input ports.
     * <BR>
     * Each SPL schema in {@code outputSchemas} an output port
     * for the operator with the output port index
     * matching the position of the schema in {@code outputSchemas}.
     * If {@code outputSchemas} is {@code null} or empty then the operator will not
     * have any output ports.
     * 
     * @param te Reference to Topology the operator will be in.
     * @param opClass Class of the operator to be invoked.
     * @param inputs Input streams to be connected to the operator. May be {@code null} if no input  streams are required.
     * @param outputSchemas Schemas of the output streams. May be {@code null} if no output streams are required.
     * @param params
     *            Parameters for the SPL Java Primitive operator, ignored if {@code null}.
     * @return List of {@code SPLStream} instances that represent the outputs of the operator.
     */
    public static List<SPLStream> invokeJavaPrimitive(
            TopologyElement te,
            Class<? extends Operator> opClass,
            List<? extends SPLInput> inputs, List<StreamSchema> outputSchemas, Map<String, ? extends Object> params) {
        
        BOperatorInvocation op = te.builder().addOperator(
                getInvocationName(opClass),
                getKind(opClass), fixParameters(params));
        op.setModel(MODEL_SPL, LANGUAGE_JAVA);
        op._json().addProperty(KIND_CLASS, opClass.getCanonicalName());
        SourceInfo.setSourceInfo(op, JavaPrimitive.class);
        
        if (inputs != null && !inputs.isEmpty()) {
            for (SPLInput input : inputs)
                SPL.connectInputToOperator(input, op);
        }
        
        if (outputSchemas == null || outputSchemas.isEmpty())
            return Collections.emptyList();
        
        List<SPLStream> streams = new ArrayList<>(outputSchemas.size());
        for (StreamSchema outputSchema : outputSchemas)
            streams.add(newSPLStream(te, op, outputSchema, outputSchemas.size() == 1));
            
        return streams;
    }

    /**
     * Invocation of a Java primitive operator that consumes a Stream.
     * 
     * @param opClass
     *            Class of the operator to be invoked
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param params
     *            Parameters for the SPL Java Primitive operator, ignored if {@code null}.
     */
    public static TSink invokeJavaPrimitiveSink(
            Class<? extends Operator> opClass, SPLInput input,
            Map<String, ? extends Object> params) {

        BOperatorInvocation sink = input.builder().addOperator(
                getInvocationName(opClass),
                getKind(opClass), fixParameters(params));
        sink.setModel(MODEL_SPL, LANGUAGE_JAVA);
        sink._json().addProperty(KIND_CLASS, opClass.getCanonicalName());
        SourceInfo.setSourceInfo(sink, JavaPrimitive.class);
        SPL.connectInputToOperator(input, sink);
        return new TSinkImpl(input.topology(), sink);
    }
    
    /**
     * Invocation of a Java primitive source operator to produce a SPL Stream.
     * 
     * @param te
     *            Reference to Topology the operator will be in.
     * @param opClass
     *            Class of the operator to be invoked.
     * @param schema
     *            Schema of the output port.
     * @param params
     *            Parameters for the SPL Java Primitive operator, ignored if {@code null}.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeJavaPrimitiveSource(TopologyElement te,
            Class<? extends Operator> opClass,
            StreamSchema schema, Map<String, ? extends Object> params) {

        
        BOperatorInvocation source = te.builder().addOperator(
                getInvocationName(opClass),
                getKind(opClass), fixParameters(params));
        source.setModel(MODEL_SPL, LANGUAGE_JAVA);
        source._json().addProperty(KIND_CLASS, opClass.getCanonicalName());
        SourceInfo.setSourceInfo(source, JavaPrimitive.class);
        return newSPLStream(te, source, schema, true);
    }
    
    
    private static String getKind(Class<?> opClass) {
        
        PrimitiveOperator primitive = opClass.getAnnotation(PrimitiveOperator.class);
        
        final String kindName = primitive.name().length() == 0 ?
                opClass.getSimpleName() : primitive.name();
        
        String namespace;
        if (primitive.namespace().length() != 0)
            namespace = primitive.namespace();
        else {
            Package pkg = opClass.getPackage();
            if (pkg != null) {
                Namespace ns = pkg.getAnnotation(Namespace.class);
                if (ns == null)
                    namespace = pkg.getName();
                else
                    namespace = ns.value();
            }
            else {
                namespace = "";
            }
        }
        
        return namespace + "::" + kindName;
    }
    
    
    private static String getInvocationName(Class<? extends Operator> opClass) {
        PrimitiveOperator po = opClass.getAnnotation(PrimitiveOperator.class);
        if (po == null)
            throw new IllegalStateException(Messages.getString("SPL_MISSING_ANNOTATION", opClass.getName()));
        String opName = po.name();
        if (opName.isEmpty()) {
            opName = opClass.getSimpleName();
        }
        return opName;
    }
}
