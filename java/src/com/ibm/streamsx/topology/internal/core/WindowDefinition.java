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
import com.ibm.streamsx.topology.logic.Identity;

public class WindowDefinition<T,K> extends TopologyItem implements TWindow<T,K> {

    private final TStream<T> stream;
    // This is the eviction policy in SPL terms
    protected final StreamWindow.Policy policy;
    protected final long config;
    protected final TimeUnit timeUnit;
        
    private final Function<? super T,? extends K> keyGetter;
    
    private WindowDefinition(TStream<T> stream, StreamWindow.Policy policy, long config, TimeUnit timeUnit, Function<? super T,? extends K> keyGetter) {
        super(stream);
        this.stream = stream;
        this.policy = policy;
        this.config = config;
        this.keyGetter = keyGetter;
        this.timeUnit = timeUnit;
        
        assert (timeUnit == null && policy != StreamWindow.Policy.TIME) ||
               (timeUnit != null && policy == StreamWindow.Policy.TIME);
    }

    public WindowDefinition(TStream<T> stream, int count) {
        this(stream, Policy.COUNT, count, null, null);
    }

    public WindowDefinition(TStream<T> stream, long time, TimeUnit unit) {
        this(stream, Policy.TIME, time, unit, null);
    }

    public WindowDefinition(TStream<T> stream, TWindow<?,?> configWindow) {
        this(stream, ((WindowDefinition<?,?>) configWindow).policy,
                ((WindowDefinition<?,?>) configWindow).config,
                ((WindowDefinition<?,?>) configWindow).timeUnit,
                null);
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
    public boolean isKeyed() {
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
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator) {
        
        java.lang.reflect.Type aggregateType = TypeDiscoverer.determineStreamType(aggregator, null);
        
        return aggregate(aggregator, aggregateType, Policy.COUNT, 1, null);
    }
    
    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            long period, TimeUnit unit) {
        if (period == 0)
            throw new IllegalArgumentException("Aggregate period cannot be zero.");
        
        java.lang.reflect.Type aggregateType = TypeDiscoverer.determineStreamType(aggregator, null);
        
        return aggregate(aggregator, aggregateType, Policy.TIME, period, unit);
    }
    
    private <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            java.lang.reflect.Type aggregateType, Policy triggerPolicy, Object triggerConfig, TimeUnit triggerTimeUnit) {
        
        if (getTupleClass() == null && !isKeyed()) {
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

        addInput(aggOp, triggerPolicy, triggerConfig, triggerTimeUnit);

        return JavaFunctional.addJavaOutput(this, aggOp, aggregateType);
    }
    
    private Map<String,Object> getOperatorParams() {
        Map<String,Object> params = new HashMap<>();
        if (isKeyed())
            params.put(FunctionWindow.WINDOW_KEY_GETTER_PARAM, ObjectUtils.serializeLogic(keyGetter));
        return params;
    }

    private BInputPort addInput(BOperatorInvocation aggOp,
            StreamWindow.Policy triggerPolicy, Object triggerConfig, TimeUnit triggerTimeUnit) {
        BInputPort bi = stream.connectTo(aggOp, true, null);
        
        
        return bi.window(Type.SLIDING, policy, config, timeUnit,
                triggerPolicy, triggerConfig, triggerTimeUnit, isKeyed());
    }
    
    public <J, U> TStream<J> joinInternal(TStream<U> xstream,
            Function<? super U, ? extends K> xstreamKey,
            BiFunction<U, List<T>, J> joiner, java.lang.reflect.Type tupleType) {
        
        String opName = LogicUtils.functionName(joiner);
        if (opName.isEmpty()) {
            opName = TypeDiscoverer.getTupleName(getTupleType()) + "Join";
        }

        Map<String, Object> params = getOperatorParams();
        if (isKeyed() && xstreamKey != null) {
            
            params.put(FunctionJoin.JOIN_KEY_GETTER_PARAM, ObjectUtils.serializeLogic(xstreamKey));
        }

        BOperatorInvocation joinOp = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionJoin.class, joiner, params);
        
        SourceInfo.setSourceInfo(joinOp, WindowDefinition.class);
               
        @SuppressWarnings("unused")
        BInputPort input0 = addInput(joinOp, Policy.COUNT, Integer.MAX_VALUE, (TimeUnit) null);

        @SuppressWarnings("unused")
        BInputPort input1 = xstream.connectTo(joinOp, true, null);

        return JavaFunctional.addJavaOutput(this, joinOp, tupleType);

    }
    
    @Override
    public <U> TWindow<T,U> key(Function<? super T, ? extends U> keyGetter) {
        if (keyGetter == null)
            throw new NullPointerException();
        return new WindowDefinition<T,U>(stream, policy, config, timeUnit, keyGetter);
    }
    @Override
    public TWindow<T, T> key() {
         return key(new Identity<T>());
    }
}
