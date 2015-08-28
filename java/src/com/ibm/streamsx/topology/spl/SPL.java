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

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.TSinkImpl;

/**
 * Integration between Java topologies and SPL operator invocations. If the SPL
 * operator to be invoked is an SPL Java primitive operator then the methods of
 * {@link JavaPrimitive} should be used.
 * <p>
 * Use {@link UString} or {@link Unsigned} class instances to pass
 * SPL operator parameters of the corresponding type.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aUStringParam", new UString(...));
 * params.put("aUShortParam", new UnsignedShort(13));
 * ... = SPLPrimitive.invokeOperator(..., params);
 * }</pre>
 * <p>
 * In addition to the usual Java types used for operator parameter values,
 * a {@code Supplier<T>} parameter value may be specified.
 * Submission time parameters are passed in this manner.
 * See {@link Topology#getSubmissionParameter(String, Class)}.
 * For example:
 * <pre>{@code
 * Map<String,Object> params = ...
 * params.put("aLongParam", topology.getSubmissionParameter(..., Long.class);
 * params.put("aShortParam", topology.getSubmissionParameter(..., (Short)13);
 * params.put("aULongParam", topology.getSubmissionParameter(..., UnsignedLong.class);
 * params.put("aUShortParam", topology.getSubmissionParameter(..., new UnsignedShort(13));
 * params.put("aUStringParam", topology.getSubmissionParameter(..., new UString(...));
 * ... = SPLPrimitive.invokeOperator(..., params);
 * }</pre>
 */
public class SPL {

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
