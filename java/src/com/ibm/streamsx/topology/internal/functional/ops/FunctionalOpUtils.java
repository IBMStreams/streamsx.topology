/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.state.CheckpointContext;
import com.ibm.streams.operator.state.CheckpointContext.Kind;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.StatelessFunctionalHandler;

class FunctionalOpUtils {
    
    
    static <T> FunctionalHandler<T> createFunctionHandler(OperatorContext context, FunctionContext functionContext, String functionalLogic) throws Exception {
        CheckpointContext cc = context.getOptionalContext(CheckpointContext.class);
        if (cc != null) {
            if (cc.getKind() == Kind.OPERATOR_DRIVEN)
                throw new IllegalStateException(); // TODO compile time checks.
            
            StatefulFunctionalHandler<T> handler = new StatefulFunctionalHandler<T>(functionContext, functionalLogic);
            
            if (handler.isStateful())
                context.registerStateHandler(handler);
            return handler;
        }
        return new StatelessFunctionalHandler<T>(functionContext, functionalLogic);
    }
    
    /**
     * Verify a functional operator is not the start of a consistent region.
     * @param checker Context checker.
     */
    static void checkNotConsistentRegionSource(OperatorContextChecker checker) {
        OperatorContext context = checker.getOperatorContext();
        ConsistentRegionContext crc = context.getOptionalContext(ConsistentRegionContext.class);
        if (crc == null)
            return;

        if (crc.isStartOfRegion() || crc.isTriggerOperator())
            checker.setInvalidContext(Messages.getString("CONSISTENT_CHECK_1"), new String[] {context.getKind()});
    }
}
