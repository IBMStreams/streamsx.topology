/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;

/**
 * Functional logic handler used when checkpointing/consistent region is configured
 * and the logic is not immutable.
 * 
 */
class StatefulFunctionalHandler<T> extends FunctionalHandler<T> implements StateHandler {
    
    private final String initialLogic;
    private T logic;

    StatefulFunctionalHandler(FunctionContext context,
            String initialLogic) throws Exception {
        super(context);
        this.initialLogic = initialLogic;
    }
    
    @Override
    public synchronized T getLogic() {
        return logic;
    }
    
    private synchronized void closeLogic() {
        if (this.logic != null) {
            // Clear mapping of custom metrics to any objects in the logic.
            ((FunctionOperatorContext) getFunctionContext()).clearMetrics();
            closeLogic(this.logic);
            this.logic = null;
        }
    }
    
    private synchronized void setLogic(T logic) {
        this.logic = logic;
    }
    
    @Override
    public void resetToInitialState() throws Exception {
        closeLogic();
        setLogic(getLogicObject(initialLogic));
        initializeLogic();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        final Object logic = getLogic();
        synchronized (logic) {
            checkpoint.getOutputStream().writeObject(logic);
        }
    }

    @Override
    public void drain() throws Exception {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        closeLogic();
        final Object logic = checkpoint.getInputStream().readObject();
        setLogic((T) logic);
        initializeLogic();
    }

    @Override
    public void retireCheckpoint(long arg0) throws Exception {
    }
}
