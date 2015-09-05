package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;

class StatefulFunctionalHandler<T> extends FunctionalHandler<T> implements StateHandler {
    
    private T logic;

    public StatefulFunctionalHandler(FunctionContext context,
            String serializedLogic) throws Exception {
        super(context, serializedLogic);
        resetToInitialState();
    }
    
    @Override
    public synchronized T getLogic() {
        return logic;
    }
    
    private void setLogic(T logic) throws Exception {
        synchronized(this) {
            this.logic = logic;
        }
        initializeLogic();
    }
    
    @Override
    public void resetToInitialState() throws Exception {
        setLogic(initialLogic());      
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        final Object logic = getLogic();
        synchronized (logic) {
            checkpoint.getOutputStream().writeObject(logic);
        }
        checkpoint.getOutputStream().writeObject(getLogic());
    }

    @Override
    public void drain() throws Exception {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        final Object logic = checkpoint.getInputStream().readObject();
        setLogic((T) logic);
    }

    @Override
    public void retireCheckpoint(long arg0) throws Exception {
    }
}
