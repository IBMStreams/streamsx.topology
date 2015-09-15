package com.ibm.streamsx.topology.internal.functional;

import com.ibm.streamsx.topology.function.FunctionContext;

public final class StatelessFunctionalHandler<T> extends FunctionalHandler<T> {

    private final T logic;
    
    public StatelessFunctionalHandler(FunctionContext context, String serializedLogic) throws Exception {
        super(context, serializedLogic);
        logic = initialLogic();
        initializeLogic();
    }
       
    public T getLogic() {
        return logic;
    }
}
