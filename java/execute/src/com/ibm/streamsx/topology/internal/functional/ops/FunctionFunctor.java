/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;
import com.ibm.streamsx.topology.internal.functional.FunctionalOpProperties;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameterManager;

/**
 * 
 * Common code for operators with inputs and outputs.
 * 
 */
@SharedLoader
public abstract class FunctionFunctor extends AbstractOperator implements Functional, Closeable {

    static final Logger trace = Logger.getLogger("com.ibm.streamsx.topology.operators");
    
    @ContextCheck(runtime=false)
    public static void checkNotConsistentRegionSource(OperatorContextChecker checker) {
        FunctionalOpUtils.checkNotConsistentRegionSource(checker);
    }

    // parameters
    private String functionalLogic;
    private String[] jar;
    private String[] submissionParamNames;
    private String[] submissionParamValues;
    
    private FunctionOperatorContext functionContext;
    
    /**
     * Logic (function) used by this operator,
     * will be closed upon shutdown.
     */
    private FunctionalHandler<?> logicHandler;

    public final String getFunctionalLogic() {
        return functionalLogic;
    }

    @Parameter
    public void setFunctionalLogic(String logic) {
        this.functionalLogic = logic;
    }

    public final String[] getJar() {
        return jar;
    }

    @Parameter(optional = true)
    public final void setJar(String[] jar) {
        this.jar = jar;
    }

    public final String[] getSubmissionParamNames() {
        return submissionParamNames;
    }

    @Parameter(optional = true)
    public final void setSubmissionParamNames(String[] SubmissionParamNames) {
        this.submissionParamNames = SubmissionParamNames;
    }

    public final String[] getSubmissionParamValues() {
        return submissionParamValues;
    }

    @Parameter(optional = true)
    public final void setSubmissionParamValues(String[] SubmissionParamValues) {
        this.submissionParamValues = SubmissionParamValues;
    }

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        FunctionalHelper.addLibraries(this, getJar());
        FunctionFunctor.initializeSubmissionParameters(context);
        functionContext = new FunctionOperatorContext(context);
    }
    
    protected FunctionContext getFunctionContext() {
        return functionContext;
    }
    
    /* Ensure any custom metric collection is completed when the operator
     * has no more work to do.
     */
    
    private AtomicInteger finalMarks = new AtomicInteger();
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
        if (mark == Punctuation.FINAL_MARKER) {
            int totalFinals = finalMarks.incrementAndGet();
            if (totalFinals == getOperatorContext().getNumberOfStreamingInputs())
                functionContext.finalMarkers();
        }
    }
    @Override
    public void allPortsReady() throws Exception {
        if (getOperatorContext().getNumberOfStreamingInputs() == 0)
            functionContext.finalMarkers();
    }
    
    @Override
    public synchronized void shutdown() throws Exception {
        close();
        
        if (logicHandler != null)
            logicHandler.close();
               
        super.shutdown();
    }
    
    public void close() throws IOException {}
    
    public <T> FunctionalHandler<T> createLogicHandler() throws Exception {
        FunctionalHandler<T> handler = FunctionalOpUtils.createFunctionHandler(
                getOperatorContext(), getFunctionContext(), getFunctionalLogic());
        this.logicHandler = handler;
        return handler;
    }

    /**
     * Initialize submission parameter value information
     * from operator context information.
     * @param context the operator context
     */
    public synchronized static void initializeSubmissionParameters(OperatorContext context) {
        // The TYPE_SPL_SUBMISSION_PARAMS parameter value is the same for
        // all operator contexts.
        if (!SubmissionParameterManager.initialized()) {
            List<String> names = context.getParameterValues(FunctionalOpProperties.NAME_SUBMISSION_PARAM_NAMES);
            if (names != null && !names.isEmpty()) {
                List<String> values = context.getParameterValues(FunctionalOpProperties.NAME_SUBMISSION_PARAM_VALUES);
                Map<String,String> map = new HashMap<>();
                for (int i = 0; i < names.size(); i++) {
                    String name = names.get(i);
                    String value = values.get(i);
                    map.put(name, value);
                }
                SubmissionParameterManager.setValues(map);
            }
        }
    }
}
