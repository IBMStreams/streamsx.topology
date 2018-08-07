/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.internal.core.JavaFunctionalOps.JOIN_KIND;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.functional.FunctionalOpProperties;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;
import com.ibm.streamsx.topology.internal.logic.ObjectUtils;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.logic.Identity;

public class WindowDefinition<T,K> extends TopologyItem implements TWindow<T,K> {

    private final TStream<T> stream;
    // This is the eviction policy in SPL terms
    protected final String policy;
    protected final long config;
    protected final TimeUnit timeUnit;
        
    private final Function<? super T,? extends K> keyGetter;
    
    private WindowDefinition(TStream<T> stream, String policy, long config, TimeUnit timeUnit, Function<? super T,? extends K> keyGetter) {
        super(stream);
        this.stream = stream;
        this.policy = policy;
        this.config = config;
        this.keyGetter = keyGetter;
        this.timeUnit = timeUnit;
        
        assert (timeUnit == null && !policy.equals(BInputPort.Window.TIME_POLICY)) ||
               (timeUnit != null && policy.equals(BInputPort.Window.TIME_POLICY));
    }

    public WindowDefinition(TStream<T> stream, int count) {
        this(stream, BInputPort.Window.COUNT_POLICY, count, null, null);
    }

    public WindowDefinition(TStream<T> stream, long time, TimeUnit unit) {
        this(stream, BInputPort.Window.TIME_POLICY, time, unit, null);
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
        
        return aggregate(aggregator, aggregateType, BInputPort.Window.COUNT_POLICY, 1, null);
    }
    
    @Override
    public <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            long period, TimeUnit unit) {
        if (period == 0)
            throw new IllegalArgumentException(Messages.getString("CORE_AGGREGATE_PERIOD_CANNOT_BE_ZERO"));
        
        java.lang.reflect.Type aggregateType = TypeDiscoverer.determineStreamType(aggregator, null);
        
        return aggregate(aggregator, aggregateType, BInputPort.Window.TIME_POLICY, period, unit);
    }
    
    private <A> TStream<A> aggregate(Function<List<T>, A> aggregator,
            java.lang.reflect.Type aggregateType, String triggerPolicy, Object triggerConfig, TimeUnit triggerTimeUnit) {
        
        if (getTupleClass() == null && !isKeyed()) {
            java.lang.reflect.Type tupleType = TypeDiscoverer.determineStreamTypeNested(Function.class, 0, List.class, aggregator);
            setPartitioned(tupleType);
        }
        
        String opName = LogicUtils.functionName(aggregator);

        BOperatorInvocation aggOp = JavaFunctional.addFunctionalOperator(this,
                opName, JavaFunctionalOps.AGGREGATE_KIND, aggregator, getOperatorParams()).layoutKind("Aggregate");
        SourceInfo.setSourceInfo(aggOp, WindowDefinition.class);

        addInput(aggOp, triggerPolicy, triggerConfig, triggerTimeUnit);

        return JavaFunctional.addJavaOutput(this, aggOp, aggregateType, true);
    }
    
    private Map<String,Object> getOperatorParams() {
        Map<String,Object> params = new HashMap<>();
        if (isKeyed())
            params.put(FunctionalOpProperties.WINDOW_KEY_GETTER_PARAM, ObjectUtils.serializeLogic(keyGetter));
        return params;
    }

    public BInputPort addInput(BOperatorInvocation aggOp,
            String triggerPolicy, Object triggerConfig, TimeUnit triggerTimeUnit) {
        BInputPort bi = stream.connectTo(aggOp, true, null);
        
        
        return bi.window(BInputPort.Window.SLIDING, policy, config, timeUnit,
                triggerPolicy, triggerConfig, triggerTimeUnit, isKeyed());
    }
    
    public <J, U> TStream<J> joinInternal(TStream<U> xstream,
            Function<? super U, ? extends K> xstreamKey,
            BiFunction<U, List<T>, J> joiner, java.lang.reflect.Type tupleType) {
        
        String opName = LogicUtils.functionName(joiner);

        Map<String, Object> params = getOperatorParams();
        if (isKeyed() && xstreamKey != null) {
            
            params.put(FunctionalOpProperties.JOIN_KEY_GETTER_PARAM, ObjectUtils.serializeLogic(xstreamKey));
        }

        BOperatorInvocation joinOp = JavaFunctional.addFunctionalOperator(this,
                opName, JOIN_KIND, joiner, params);
        
        SourceInfo.setSourceInfo(joinOp, WindowDefinition.class);
               
        @SuppressWarnings("unused")
        BInputPort input0 = addInput(joinOp, BInputPort.Window.COUNT_POLICY, Integer.MAX_VALUE, (TimeUnit) null);

        @SuppressWarnings("unused")
        BInputPort input1 = xstream.connectTo(joinOp, true, null);

        return JavaFunctional.addJavaOutput(this, joinOp, tupleType, true);

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
