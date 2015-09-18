/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;

/**
 * 
 * Common code for operators with inputs and outputs.
 * 
 */
@SharedLoader
public abstract class FunctionFunctor extends AbstractOperator implements Functional {

    public static final String FUNCTIONAL_LOGIC_PARAM = "functionalLogic";
    static final Logger trace = Logger.getLogger("com.ibm.streamsx.topology.operators");

    // parameters
    private String functionalLogic;
    private String[] jar;
    private String[] submissionParamNames;
    private String[] submissionParamValues;
    
    private FunctionContext functionContext;
    
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
        SubmissionParameterManager.initialize(context);
        functionContext = new FunctionOperatorContext(context);
    }
    
    FunctionContext getFunctionContext() {
        return functionContext;
    }
    
    @Override
    public synchronized void shutdown() throws Exception {
        if (logicHandler != null)
            logicHandler.close();
        super.shutdown();
    }
    
    public <T> FunctionalHandler<T> createLogicHandler() throws Exception {
        FunctionalHandler<T> handler = FunctionalOpUtils.createFunctionHandler(
                getOperatorContext(), getFunctionContext(), getFunctionalLogic());
        this.logicHandler = handler;
        return handler;
    }
}
