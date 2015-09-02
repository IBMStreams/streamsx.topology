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
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Initializable;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;
import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

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

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        FunctionalHelper.addLibraries(this, getJar());
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
        FunctionalHandler<T> handler = new FunctionalHandler<T>(getFunctionContext(), getFunctionalLogic());
        this.logicHandler = handler;
        return handler;
    }
    
    /*
    
    public void setLogicHandler(FunctionalHandler<?> logicHandler) throws Exception {
        this.logicHandler = logicHandler;
    } 
    */
    
    static void initializeLogic(OperatorContext context, Object logicInstance) throws Exception {
        for (;;) {
            if (logicInstance instanceof Initializable) {
                ((Initializable) logicInstance).initialize(new FunctionOperatorContext(context));
            }
            if (logicInstance instanceof WrapperFunction) {
                logicInstance = ((WrapperFunction) logicInstance).getWrappedFunction();
            } else {
                break;
            }
        }
    }
    
    /**
     * If logicInstance implements AutoCloseable
     * then shut it down by calling it close() method.
     */
    static void closeLogic(Object logicInstance) {
        for (;;) {
            if (logicInstance instanceof AutoCloseable) {
                try {
                    synchronized (logicInstance) {
                        ((AutoCloseable) logicInstance).close();
                    }
                } catch (Exception e) {
                    trace.log(TraceLevel.ERROR, "Exception " + e.getMessage()
                            + " closing function instance:" + logicInstance, e);
                }
            }
            if (logicInstance instanceof WrapperFunction) {
                logicInstance = ((WrapperFunction) logicInstance).getWrappedFunction();
            } else {
                break;
            }
        }       
    }
}
