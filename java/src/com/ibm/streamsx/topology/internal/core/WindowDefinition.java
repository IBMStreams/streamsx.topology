/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Policy;
import com.ibm.streams.operator.window.StreamWindow.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionAggregate;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionJoin;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;
import com.ibm.streamsx.topology.tuple.Keyable;

public class WindowDefinition<T> extends TopologyItem implements TWindow<T> {

    private final TStream<T> stream;
    // This is the eviction policy in SPL terms
    protected final StreamWindow.Policy policy;
    protected final long config;

    public WindowDefinition(TStream<T> stream, int count) {
        super(stream);
        this.stream = stream;
        this.policy = Policy.COUNT;
        this.config = count;
    }

    public WindowDefinition(TStream<T> stream, long time, TimeUnit unit) {
        super(stream);
        this.stream = stream;
        this.policy = Policy.TIME;
        this.config = unit.toMillis(time);
    }

    public WindowDefinition(TStream<T> stream, TWindow<?> configWindow) {
        super(stream);
        this.stream = stream;
        this.policy = ((WindowDefinition<?>) configWindow).policy;
        this.config = ((WindowDefinition<?>) configWindow).config;
    }

    @Override
    public TStream<T> getStream() {
        return stream;
    }


    @Override
    public Class<T> getTupleClass() {
        return stream.getTupleClass();
    }

    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            Class<A> tupleClass) {
        
        return aggregate(aggregator, tupleClass, Policy.COUNT, 1);
    }
    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            long period, TimeUnit unit, Class<A> tupleClass) {
        return aggregate(aggregator, tupleClass, Policy.TIME, unit.toMillis(period));
    }
    
    private <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            Class<A> tupleClass, Policy triggerPolicy, Object triggerConfig) {
        
        String opName = LogicUtils.functionName(aggregator);
        if (opName.isEmpty()) {
            opName = getTupleClass().getSimpleName() + "Aggregate";
        }

        BOperatorInvocation aggOp = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionAggregate.class, aggregator);
        SourceInfo.setSourceInfo(aggOp, WindowDefinition.class);

        addInput(aggOp, triggerPolicy, triggerConfig);

        return JavaFunctional.addJavaOutput(this, aggOp, tupleClass);
    }

    public BInputPort addInput(BOperatorInvocation aggOp,
            StreamWindow.Policy triggerPolicy, Object triggerConfig) {
        BInputPort bi = stream.connectTo(aggOp, true, null);
        return bi.window(Type.SLIDING, policy, config, triggerPolicy,
                triggerConfig, Keyable.class.isAssignableFrom(getTupleClass()));

    }

    @Override
    public <J, U> TStream<J> join(TStream<U> xstream,
            BiFunction<U, Iterable<T>, J> joiner, Class<J> tupleClass) {
        
        String opName = LogicUtils.functionName(joiner);
        if (opName.isEmpty()) {
            opName = getTupleClass().getSimpleName() + "Join";
        }

        BOperatorInvocation joinOp = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionJoin.class, joiner);
        
        SourceInfo.setSourceInfo(joinOp, WindowDefinition.class);
               
        @SuppressWarnings("unused")
        BInputPort input0 = addInput(joinOp, Policy.COUNT, Integer.MAX_VALUE);

        @SuppressWarnings("unused")
        BInputPort input1 = xstream.connectTo(joinOp, true, null);

        return JavaFunctional.addJavaOutput(this, joinOp, tupleClass);

    }
}
