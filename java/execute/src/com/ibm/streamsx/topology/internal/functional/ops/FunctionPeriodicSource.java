/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.samples.patterns.PollingTupleProducer;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator
@OutputPortSet(cardinality = 1)
@SharedLoader
public class FunctionPeriodicSource extends PollingTupleProducer implements Functional {
    
    @ContextCheck(runtime=false)
    public static void checkNotConsistentRegionSource(OperatorContextChecker checker) {
        FunctionalOpUtils.checkNotConsistentRegionSource(checker);
    }

    private FunctionalHandler<Supplier<Iterable<Object>>> dataHandler;
    private SPLMapping<Object> mapping;

    private String functionalLogic;
    private String[] jar;
    private String[] submissionParamNames;
    private String[] submissionParamValues;
    private StreamingOutput<OutputTuple> output;
    
    private FunctionContext functionContext;

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        FunctionalHelper.addLibraries(this, getJar());
        FunctionFunctor.initializeSubmissionParameters(context);
        functionContext = new FunctionOperatorContext(context);
        
        output = getOutput(0);
        mapping = getOutputMapping(this, 0);
        
        dataHandler = FunctionalOpUtils.createFunctionHandler(
                getOperatorContext(), getFunctionContext(), getFunctionalLogic());
    }
    
    FunctionContext getFunctionContext() {
        return functionContext;
    }

    public String getFunctionalLogic() {
        return functionalLogic;
    }

    @Parameter
    public void setFunctionalLogic(String logic) {
        this.functionalLogic = logic;
    }

    public String[] getJar() {
        return jar;
    }

    @Parameter(optional = true)
    public void setJar(String[] jar) {
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
    protected void fetchTuples() throws Exception {

        for (Object tuple : dataHandler.getLogic().get()) {
            if (Thread.interrupted())
                return;
            if (tuple == null)
                continue;
            output.submit(mapping.convertTo(tuple));
        }
    }
    
    @Override
    public void shutdown() throws Exception {
        dataHandler.close();
        super.shutdown();
    }
}
