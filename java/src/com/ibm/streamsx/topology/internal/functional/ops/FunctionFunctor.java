/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
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
    
    /**
     * Logic (function) used by this operator,
     * will be closed upon shutdown.
     */
    private Object logicInstance;

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

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        FunctionalHelper.addLibraries(this, getJar());
    }
    
    @Override
    public void shutdown() throws Exception {
        closeLogic(logicInstance);
        super.shutdown();
    }
    
    public void setLogic(Object logicInstance) {
        this.logicInstance = logicInstance;
    }
    
    /**
     * If logicInstance implements AutoCloseable
     * then shut it down by calling it close() method.
     */
    static void closeLogic(Object logicInstance) {
        if (logicInstance instanceof AutoCloseable) {
            try {
                synchronized (logicInstance) {
                    ((AutoCloseable) logicInstance).close();
                }
            } catch (Exception e) {
                trace.log(TraceLevel.ERROR,
                        "Exception " +  e.getMessage() + " closing function instance:" + logicInstance, e);
            }
        }       
    }
}
