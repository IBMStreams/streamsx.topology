/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@PrimitiveOperator
@OutputPortSet(cardinality = 1)
@SharedLoader
public class FunctionSource extends ProcessTupleProducer implements Functional {

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
        SubmissionParameterManager.initialize(context);

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
    protected void process() throws Exception {

        try {
            Supplier<Iterable<Object>> data = dataHandler.getLogic();
            for (Object tuple : data.get()) {
                if (Thread.interrupted())
                    return;
                if (tuple == null)
                    continue;
                output.submit(mapping.convertTo(tuple));
            }
        } finally {
            dataHandler.close();
            dataHandler = null;
        }
        output.punctuate(StreamingData.Punctuation.FINAL_MARKER);
    }
    
    @Override
    public void shutdown() throws Exception {
        if (dataHandler != null)
             dataHandler.close();
        super.shutdown();
    }
}
