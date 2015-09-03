/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.internal.core.InternalProperties.TK_DIRS;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.SubmissionParameter;
import com.ibm.streamsx.topology.internal.core.TSinkImpl;

/**
 * Integration between Java topologies and SPL operator invocations. If the SPL
 * operator to be invoked is an SPL Java primitive operator then the methods of
 * {@link JavaPrimitive} should be used.
 * <p>
 * When necessary use {@code createValue(T, MetaType)}
 * to create parameter values for SPL types.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aInt32", 13);
 * params.put("aUInt32", SPL.createValue(13, MetaType.UINT32));
 * params.put("aRString", "some string value");
 * params.put("aUString", SPL.createValue("some ustring value", MetaType.USTRING));
 * ... = SPLPrimitive.invokeOperator(..., params);
 * }</pre>
 * <p>
 * In addition to the usual Java types used for operator parameter values,
 * a {@code Supplier<T>} parameter value may be specified.
 * Submission time parameters are passed in this manner.
 * See {@link #createSubmissionParameter(Topology, String, Object, boolean)}.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aInt32", topology.createSubmissionParameter("int32Param", 13);
 * params.put("aUInt32", SPL.createSubmissionParameter(topology, "uint32Param", SPL.createValue(13, MetaType.UINT32), true);
 * params.put("aRString", topology.createSubmissionParameter("rstrParam", "some string value");
 * params.put("aUString", SPL.createSubmissionParameter(topology, "ustrParam", SPL.createValue("some ustring value", MetaType.USTRING), true);
 * params.put("aUInt8", SPL.createSubmissionParameter(topology, "uint8Param", SPL.createValue((byte)13, MetaType.UINT8), true);
 * ... = SPLPrimitive.invokeOperator(..., params);
 * }</pre>
 */
public class SPL {
    
    /**
     * Create a SPL parameter value wrapper object for the 
     * specified SPL {@code MetaType}.
     * <p>
     * Use of this is required to construct a SPL operator parameter value
     * whose SPL type is not implied from simple Java type.  e.g.,
     * a {@code String} value is interpreted as a SPL {@code rstring},
     * and {@code Byte,Short,Integer,Long} are interpreted as SPL signed integers.
     * Hence, this must be used for SPL {@code ustring} and unsigned integers.
     * @param value the value to wrap
     * @param metaType the SPL meta type
     * @return the wrapper object
     * @throws IllegalArgumentException if value is null or its class is
     *     not appropriate for {@code metaType}
     * @see #paramValueToString(Object)
     */
    public static <T> Object createValue(T value, MetaType metaType) {
        return new SPLValue<T>(value, metaType).toJSON();
    }
    
    private static SPLValue<?> createSPLValue(Object paramValue) {
        if (paramValue instanceof JSONObject) {
            SPLValue<?> splValue = SPLValue.fromJSON((JSONObject) paramValue);
            return splValue;
        }            
        throw new IllegalArgumentException("param is not from createValue()");
    }

    /**
     * Convert a value from {@code createValue()} to a string
     * appropriate for the wrapped value.
     * @param paramValue the parameter value  
     * @return the string
     * @throws IllegalArgumentException if {@code paramValue} is not a
     *      value from {@code createValue()}.
     */
    public static Object paramValueToString(Object paramValue) {
        SPLValue<?> splValue = createSPLValue(paramValue);
        return splValue.toString();
    }

    /**
     * Create a submission parameter with or without a default value.
     * <p>
     * Use of this is required to construct a submission parameter for
     * a SPL operator parameter whose SPL type requires the use of
     * {@code createValue(Object, MetaType)}.
     * <p>
     * See {@link Topology#createSubmissionParameter(String, Class)} for
     * general information about submission parameters.
     * 
     * @param top the topology
     * @param name the submission parameter name
     * @param paramValue a value from {@code createValue()}
     * @param withDefault true to create a submission parameter with
     *      a default value, false to create one without a default value
     *      When false, the paramValue's wrapped value's value in ignored. 
     * @return the {@code Supplier<T>} for the submission parameter
     * @throws IllegalArgumentException if {@code paramValue} is not a
     *      value from {@code createValue()}.
     */
    public static <T> Supplier<T> createSubmissionParameter(Topology top,
            String name, Object paramValue, boolean withDefault) {
        SPLValue<?> splValue = createSPLValue(paramValue);
        SubmissionParameter<T> sp = new SubmissionParameter<T>(name, splValue.toJSON(), withDefault);
        top.builder().createSubmissionParameter(name, sp.toJSON());
        return sp;
    }
    
    /**
     * Create an SPLStream from the invocation of an SPL operator
     * that consumes a stream.
     * 
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param outputSchema
     *            SPL schema of the operator's only output port.
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeOperator(String kind, SPLInput input,
            StreamSchema outputSchema, Map<String, ? extends Object> params) {

        BOperatorInvocation op = input.builder().addSPLOperator(
                opNameFromKind(kind), kind, params);
        SourceInfo.setSourceInfo(op, SPL.class);
        SPL.connectInputToOperator(input, op);
        BOutputPort stream = op.addOutput(outputSchema);
        return new SPLStreamImpl(input, stream);
    }

    public static SPLStream invokeOperator(String name, String kind,
            SPLInput input,
            StreamSchema outputSchema, Map<String, ? extends Object> params) {

        BOperatorInvocation op = input.builder().addSPLOperator(name, kind, params);
        SourceInfo.setSourceInfo(op, SPL.class);
        SPL.connectInputToOperator(input, op);
        BOutputPort stream = op.addOutput(outputSchema);
        return new SPLStreamImpl(input, stream);
    }

    /**
     * Connect an input to an operator invocation, including making the input
     * windowed if it is an instance of SPLWindow.
     */
    static BInputPort connectInputToOperator(SPLInput input,
            BOperatorInvocation op) {
        BInputPort inputPort = input.getStream().connectTo(op, false, null);
        if (input instanceof SPLWindow) {
            ((SPLWindowImpl) input).windowInput(inputPort);
        }
        return inputPort;
    }

    /**
     * Invocation an SPL operator that consumes a Stream.
     * 
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return the sink element
     */
    public static TSink invokeSink(String kind, SPLInput input,
            Map<String, ? extends Object> params) {

        return invokeSink(opNameFromKind(kind), kind, input, params);
    }

    /**
     * Invocation an SPL operator that consumes a Stream.
     * 
     * @param name
     *            Name of the operator
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param input
     *            Stream that will be connected to the only input port of the
     *            operator
     * @param params
     *            Parameters for the SPL operator, ignored if it is null.
     * @return the sink element
     */
    public static TSink invokeSink(String name, String kind, SPLInput input,
            Map<String, ? extends Object> params) {

        BOperatorInvocation op = input.builder().addSPLOperator(name, kind,
                params);
        SourceInfo.setSourceInfo(op, SPL.class);
        SPL.connectInputToOperator(input, op);
        return new TSinkImpl(input.topology(), op);
    }
    
    private static String opNameFromKind(String kind) {
        String opName;
        if (kind.contains("::"))
            opName = kind.substring(kind.lastIndexOf(':') + 1);
        else
            opName = kind;
        return opName;
    }

    /**
     * Invocation an SPL source operator to produce a Stream.
     * 
     * @param te
     *            Reference to Topology the operator will be in.
     * @param kind
     *            SPL kind of the operator to be invoked.
     * @param params
     *            Parameters for the SPL operator.
     * @param schema
     *            Schema of the output port.
     * @return SPLStream the represents the output of the operator.
     */
    public static SPLStream invokeSource(TopologyElement te, String kind,
            Map<String, Object> params, StreamSchema schema) {

        BOperatorInvocation splSource = te.builder().addSPLOperator(
                opNameFromKind(kind), kind, params);
        SourceInfo.setSourceInfo(splSource, SPL.class);
        BOutputPort stream = splSource.addOutput(schema);
       
        return new SPLStreamImpl(te, stream);
    }


    
    public static void addToolkit(TopologyElement te, File toolkitRoot) throws IOException {
        @SuppressWarnings("unchecked")
        Set<String> tks = (Set<String>) te.topology().getConfig().get(TK_DIRS);
        
        if (tks == null) {
            tks = new HashSet<>();
            te.topology().getConfig().put(TK_DIRS, tks);
        }
        tks.add(toolkitRoot.getCanonicalPath());
    }
}
