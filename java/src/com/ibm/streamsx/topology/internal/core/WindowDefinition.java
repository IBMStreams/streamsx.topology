/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Policy;
import com.ibm.streams.operator.window.StreamWindow.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.functional.ObjectUtils;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionAggregate;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionJoin;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionWindow;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;

public class WindowDefinition<T,K> extends TopologyItem implements TWindow<T,K> {

    private final TStream<T> stream;
    // This is the eviction policy in SPL terms
    protected final StreamWindow.Policy policy;
    protected final long config;
        
    private final Function<T,?> keyGetter;
    
    private WindowDefinition(TStream<T> stream, StreamWindow.Policy policy, long config, Function<T,?> keyGetter) {
        super(stream);
        this.stream = stream;
        this.policy = policy;
        this.config = config;
        this.keyGetter = keyGetter;
    }

    public WindowDefinition(TStream<T> stream, int count) {
        this(stream, Policy.COUNT, count, null);
    }

    public WindowDefinition(TStream<T> stream, long time, TimeUnit unit) {
        this(stream, Policy.TIME, unit.toMillis(time), null);
    }

    public WindowDefinition(TStream<T> stream, TWindow<?,?> configWindow) {
        this(stream, ((WindowDefinition<?,?>) configWindow).policy, ((WindowDefinition<?,?>) configWindow).config, null);
    }
    
    private final void setPartitioned(final java.lang.reflect.Type type) {

        if (type instanceof Class) {
            topology().addClassDependency((Class<?>) type);
            return;
        }
        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            java.lang.reflect.Type rawType = pt.getRawType();
            if (rawType instanceof Class) {
                topology().addClassDependency((Class<?>) rawType);
                return;
            }    
        }
    }
    
    @Override
    public boolean isPartitioned() {
        return keyGetter != null;
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
    public java.lang.reflect.Type getTupleType() {
        return stream.getTupleType();
    }

    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            Class<A> tupleClass) {
        
        return aggregate(aggregator, tupleClass, Policy.COUNT, 1);
    }
    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator) {
        
        java.lang.reflect.Type aggregateType = TypeDiscoverer.determineStreamType(aggregator, null);
        
        return aggregate(aggregator, aggregateType, Policy.COUNT, 1);
    }
    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            long period, TimeUnit unit, Class<A> tupleClass) {
        return aggregate(aggregator, tupleClass, Policy.TIME, unit.toMillis(period));
    }
    
    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            long period, TimeUnit unit) {
        java.lang.reflect.Type aggregateType = TypeDiscoverer.determineStreamType(aggregator, null);
        
        return aggregate(aggregator, aggregateType, Policy.TIME, unit.toMillis(period));
    }
    
    private <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            java.lang.reflect.Type aggregateType, Policy triggerPolicy, Object triggerConfig) {
        
        if (getTupleClass() == null && !isPartitioned()) {
            java.lang.reflect.Type tupleType = TypeDiscoverer.determineStreamTypeNested(Function.class, 0, List.class, aggregator);
            setPartitioned(tupleType);
        }
        
        String opName = LogicUtils.functionName(aggregator);
        if (opName.isEmpty()) {
            opName = TypeDiscoverer.getTupleName(getTupleType()) + "Aggregate";
        }

        
        BOperatorInvocation aggOp = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionAggregate.class, aggregator, getOperatorParams());
        SourceInfo.setSourceInfo(aggOp, WindowDefinition.class);

        addInput(aggOp, triggerPolicy, triggerConfig);

        return JavaFunctional.addJavaOutput(this, aggOp, aggregateType);
    }
    
    private Map<String,Object> getOperatorParams() {
        Map<String,Object> params = new HashMap<>();
        if (isPartitioned())
            params.put(FunctionWindow.KEY_GETTER_PARAM, ObjectUtils.serializeLogic(keyGetter));
        return params;
    }

    public BInputPort addInput(BOperatorInvocation aggOp,
            StreamWindow.Policy triggerPolicy, Object triggerConfig) {
        BInputPort bi = stream.connectTo(aggOp, true, null);
        
        
        return bi.window(Type.SLIDING, policy, config, triggerPolicy,
                triggerConfig, isPartitioned());
    }

    @Override
    public <J, U> TStream<J> join(TStream<U> xstream,
            BiFunction<U, List<T>, J> joiner, Class<J> tupleClass) {
        
        String opName = LogicUtils.functionName(joiner);
        if (opName.isEmpty()) {
            opName = getTupleClass().getSimpleName() + "Join";
        }

        BOperatorInvocation joinOp = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionJoin.class, joiner, getOperatorParams());
        
        SourceInfo.setSourceInfo(joinOp, WindowDefinition.class);
               
        @SuppressWarnings("unused")
        BInputPort input0 = addInput(joinOp, Policy.COUNT, Integer.MAX_VALUE);

        @SuppressWarnings("unused")
        BInputPort input1 = xstream.connectTo(joinOp, true, null);

        return JavaFunctional.addJavaOutput(this, joinOp, tupleClass);

    }
    
    public <J, U> TStream<J> joinInternal(TStream<U> xstream,
            BiFunction<U, List<T>, J> joiner, java.lang.reflect.Type tupleType) {
        
        String opName = LogicUtils.functionName(joiner);
        if (opName.isEmpty()) {
            opName = getTupleClass().getSimpleName() + "Join";
        }

        BOperatorInvocation joinOp = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionJoin.class, joiner, getOperatorParams());
        
        SourceInfo.setSourceInfo(joinOp, WindowDefinition.class);
               
        @SuppressWarnings("unused")
        BInputPort input0 = addInput(joinOp, Policy.COUNT, Integer.MAX_VALUE);

        @SuppressWarnings("unused")
        BInputPort input1 = xstream.connectTo(joinOp, true, null);

        return JavaFunctional.addJavaOutput(this, joinOp, tupleType);

    }
    
    @Override
    public <U> TWindow<T,U> key(Function<T, U> keyGetter) {
        if (keyGetter == null)
            throw new NullPointerException();
        return new WindowDefinition<T,U>(stream, policy, config, keyGetter);
    }
}
