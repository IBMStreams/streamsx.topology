/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;
import static com.ibm.streamsx.topology.internal.functional.ops.FunctionFunctor.trace;

import java.util.logging.Level;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.state.CheckpointContext;
import com.ibm.streams.operator.state.CheckpointContext.Kind;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.functional.StatelessFunctionalHandler;
import com.ibm.streamsx.topology.internal.logic.ObjectUtils;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.spi.runtime.TupleSerializer;

class FunctionalOpUtils {
    
    static <T> FunctionalHandler<T> createFunctionHandler(OperatorContext context, FunctionContext functionContext, String functionalLogic) throws Exception {

        final T initialLogic = getLogicObject(functionalLogic);
               
        CheckpointContext cc = context.getOptionalContext(CheckpointContext.class);
        if (trace.isLoggable(Level.FINE))
            trace.fine("Checkpoint context:" + (cc == null ? "NONE" : cc.getKind()));
        
        if (cc != null) {
            if (cc.getKind() == Kind.OPERATOR_DRIVEN)
                throw new IllegalStateException(); // TODO compile time checks.
        }
        
        ConsistentRegionContext crc = context.getOptionalContext(ConsistentRegionContext.class);
        if (crc != null) {
            if (crc.isStartOfRegion())
                throw new IllegalStateException(); // TODO compile time checks.
        }
               
        if (cc != null || crc != null) {
            
            if (!ObjectUtils.isImmutable(initialLogic.getClass())) {
                if (trace.isLoggable(Level.FINE)) {
                    if (cc != null)
                        trace.fine("Checkpoint stateful function:" + initialLogic.getClass().getName());
                    if (crc != null)
                        trace.fine("Consistent region stateful function:" + initialLogic.getClass().getName());
                }
                
                // Close it just in case it does something in its deserialization.
                FunctionalHandler.closeLogic(initialLogic); 
                                
                StatefulFunctionalHandler<T> handler =
                        new StatefulFunctionalHandler<T>(functionContext, functionalLogic);
                
                context.registerStateHandler(handler);
                
                // On any other restart we expect a reset call, but we use
                // reset calls as the common setup for the logic including
                // its initialization.
                if (context.getPE().getRelaunchCount() == 0)
                    handler.resetToInitialState();
                return handler;
            }
        }
        
        if (trace.isLoggable(Level.FINE))
            trace.fine("Stateless function:" + initialLogic.getClass().getName());
        
        return new StatelessFunctionalHandler<T>(functionContext, initialLogic);
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
    
    static TupleSerializer createTupleSerializer(String tupleSerializer) throws ClassNotFoundException {
        return (TupleSerializer) ObjectUtils.deserializeLogic(tupleSerializer);
    }
    
    static Exception throwError(Throwable t) {
        if (t instanceof Error)
            throw (Error) t;
        return (Exception) t;
    }
}
