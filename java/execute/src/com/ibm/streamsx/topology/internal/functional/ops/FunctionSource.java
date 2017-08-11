/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;
import static com.ibm.streamsx.topology.internal.functional.ops.FunctionalOpUtils.throwError;

import java.io.Closeable;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.spi.operators.FunctionalOperator;

public abstract class FunctionSource extends ProcessTupleProducer implements Functional, Closeable, FunctionalOperator {
    
    @ContextCheck(runtime=false)
    public static void checkNotConsistentRegionSource(OperatorContextChecker checker) {
        FunctionalOpUtils.checkNotConsistentRegionSource(checker);
    }

    private FunctionalHandler<Supplier<Iterable<Object>>> dataHandler;
    private SPLMapping<Object> mapping;

    private String functionalLogic;
    private String outputSerializer;
    private String[] jar;
    private String[] submissionParamNames;
    private String[] submissionParamValues;
    private StreamingOutput<OutputTuple> output;
     
    private FunctionOperatorContext functionContext;
    
    @Override
    public final synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);

        try {
            FunctionalHelper.addLibraries(this, getJar());
            FunctionFunctor.initializeSubmissionParameters(context);

            functionContext = new FunctionOperatorContext(context);
            
            output = getOutput(0);
                
            mapping = getOutputMapping(this, 0, outputSerializer);
            
            dataHandler = FunctionalOpUtils.createFunctionHandler(
                    getOperatorContext(), getFunctionContext(), getFunctionalLogic());
            
            
            initialize();
        } catch (Exception e) {
            throw throwError(exception(e));
        }
    }
    
    public FunctionContext getFunctionContext() {
        return functionContext;
    }
    
    protected Supplier<Iterable<Object>> getLogic() {
        return dataHandler.getLogic();
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
    
    @Parameter(optional=true)
    public final void setOutputSerializer(String outputSerializer) {
        this.outputSerializer = outputSerializer;
    }

    @Override
    protected final void process() throws Exception {

        try {
            Supplier<Iterable<Object>> data = getLogic();
            for (Object tuple : data.get()) {
                if (Thread.interrupted())
                    return;
                if (tuple == null)
                    continue;
                output.submit(mapping.convertTo(tuple));
            }
        } catch (Exception e) {
            throw throwError(exception(e));
        } finally {
            dataHandler.close();
            dataHandler = null;
        }
        output.punctuate(StreamingData.Punctuation.FINAL_MARKER);
        functionContext.finalMarkers();
    }
    
    @Override
    public void shutdown() throws Exception {
        try {
            close();
            
            if (dataHandler != null)
                 dataHandler.close();
        } catch (Exception e) {
            throw throwError(exception(e));
        }
        
        super.shutdown();
    }
}
