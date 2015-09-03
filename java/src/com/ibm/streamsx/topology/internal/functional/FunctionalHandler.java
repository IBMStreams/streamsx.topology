package com.ibm.streamsx.topology.internal.functional;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import java.io.IOException;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Initializable;
import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

public class FunctionalHandler<T> {

    private final FunctionContext context;
    private final String serializedLogic;
    private T logic;
    
    public FunctionalHandler(FunctionContext context, String serializedLogic) throws Exception {
        this.context = context;
        this.serializedLogic = serializedLogic;
        resetToInitialState();
    }
    
    public FunctionContext getFunctionContext() {
        return context;
    }
    
    public void resetToInitialState() throws Exception {
        T initialLogic = getLogicObject(serializedLogic);
        setLogic(initialLogic);
    }
    
    public void setLogic(T logic) throws Exception {
        synchronized (this) {
            this.logic = logic;
        }
        initializeLogic(getFunctionContext(), getLogic());
    }
       
    public synchronized T getLogic() {
        return logic;
    }
    
    public void close() throws IOException {       
        closeLogic(getLogic());
    }
        
    static void initializeLogic(FunctionContext context, Object logicInstance) throws Exception {
        for (;;) {
            if (logicInstance instanceof Initializable) {
                ((Initializable) logicInstance).initialize(context);
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
                    FunctionalHelper.trace.log(TraceLevel.ERROR, "Exception " + e.getMessage()
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
