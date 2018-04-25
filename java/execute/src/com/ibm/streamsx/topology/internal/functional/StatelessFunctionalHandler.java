/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional;

import com.ibm.streamsx.topology.function.FunctionContext;

/**
 * Functional logic handler used when 
 * the logic is immutable
 * or checkpointing/consistent region is not configured.
 * 
 * The logic's initialization occurs at operator initialization time.
 */
public final class StatelessFunctionalHandler<T> extends FunctionalHandler<T> {
    
    private final T logic;
   
    public StatelessFunctionalHandler(FunctionContext context, T initialLogic) throws Exception {
        super(context);
        this.logic = initialLogic;
        initializeLogic();
    }
    
    @Override
    public T getLogic() {
        return logic;
    }
}
