/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static java.util.Collections.singletonMap;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.TKeyedStream;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionFilter;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionMultiTransform;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionSink;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionSplit;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionTransform;
import com.ibm.streamsx.topology.internal.functional.ops.HashAdder;
import com.ibm.streamsx.topology.internal.functional.ops.HashRemover;
import com.ibm.streamsx.topology.internal.logic.FirstOfSecondParameterIterator;
import com.ibm.streamsx.topology.internal.logic.ObjectHasher;
import com.ibm.streamsx.topology.internal.logic.Print;
import com.ibm.streamsx.topology.internal.logic.RandomSample;
import com.ibm.streamsx.topology.internal.logic.Throttle;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.logic.Identity;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;

public class StreamImpl<T> extends TupleContainer<T> implements TStream<T> {

    private final BOutput output;

    @Override
    public BOutput output() {
        return output;
    }

    public StreamImpl(TopologyElement te, BOutput output, Type tupleType) {
        super(te, tupleType);
        this.output = output;
    }
    
    /**
     * Get a simple name to be used for naming operators,
     * returns Object when the class name is not used.
     */
    protected String getTupleName() {
        return TypeDiscoverer.getTupleName(getTupleType());
    }
    
    @Override
    public TStream<T> filter(Predicate<T> filter) {
        String opName = filter.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = getTupleName() + "Filter";         
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionFilter.class, filter);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        
       return addMatchingOutput(bop, refineType(Predicate.class, 0, filter));
    }
    
    protected TStream<T> addMatchingOutput(BOperatorInvocation bop, Type tupleType) {
        return JavaFunctional.addJavaOutput(this, bop, tupleType);
    }
    protected TStream<T> addMatchingStream(BOutput output) {
        return new StreamImpl<T>(this, output, getTupleType());
    }
    
    /**
     * Try to refine a type down to a Class without generics.
     */
    private Type refineType(Class<?> interfaceClass, int arg, Object object) {
        Type tupleType = getTupleType();
        if (!(tupleType instanceof Class)) {
            // try and refine the type down.
            Type type = TypeDiscoverer.determineStreamTypeFromFunctionArg(interfaceClass, arg, object);
            if (type instanceof Class)
                tupleType = type;
        }
        return tupleType;
    }

    @Override
    public TSink sink(Consumer<T> sinker) {
        
        String opName = sinker.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = getTupleName() + "Sink";
        }

        BOperatorInvocation sink = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionSink.class, sinker);
        SourceInfo.setSourceInfo(sink, StreamImpl.class);
        connectTo(sink, true, null);
        return new TSinkImpl(this, sink);
    }

    @Override
    public <U> TStream<U> transform(Function<T, U> transformer,
            Class<U> tupleTypeClass) {
        
        Type tupleType = TypeDiscoverer.determineStreamType(transformer, tupleTypeClass);     
        return _transform(transformer, tupleType);
    }
    
    @Override
    public <U> TStream<U> transform(Function<T, U> transformer) {
        return transform(transformer, null);
    }
    
    private <U> TStream<U> _transform(Function<T, U> transformer, Type tupleType) {
                
        String opName = transformer.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = TypeDiscoverer.getTupleName(tupleType) + "Transform" +
                        getTupleName();                
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionTransform.class, transformer);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        BInputPort inputPort = connectTo(bop, true, null);
        // By default add a queue
        inputPort.addQueue(true);
        return JavaFunctional.addJavaOutput(this, bop, tupleType);
    }
    
    private TStream<T> _modify(UnaryOperator<T> transformer, Type tupleType) {
        
        String opName = transformer.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = getTupleName() + "Modify";
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionTransform.class, transformer);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        BInputPort inputPort = connectTo(bop, true, null);
        // By default add a queue
        inputPort.addQueue(true);
        return this.addMatchingOutput(bop, tupleType);
    }
    


    @Override
    public TStream<T> modify(UnaryOperator<T> modifier) {
        return _modify(modifier, refineType(UnaryOperator.class, 0, modifier));
    }

    @Override
    public <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer,
            Class<U> tupleTypeClass) {
        Type tupleType = tupleTypeClass != null ? tupleTypeClass :
                   TypeDiscoverer.determineStreamTypeNested(Function.class, 1, Iterable.class, transformer);    
        return _multiTransform(transformer, tupleType);
    }
    @Override
    public <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer) {
        
        return multiTransform(transformer, null);
    }
    
    private <U> TStream<U> _multiTransform(Function<T, Iterable<U>> transformer, Type tupleType) {
    
        String opName = transformer.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = TypeDiscoverer.getTupleName(tupleType) + "MultiTransform" +
                        getTupleName();                
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                FunctionMultiTransform.class, transformer);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        BInputPort inputPort = connectTo(bop, true, null);
        // By default add a queue
        inputPort.addQueue(true);

        return JavaFunctional.addJavaOutput(this, bop, tupleType);
    }

    @Override
    public TStream<T> union(TStream<T> other) {
        if (other == this)
            return this;
        return union(Collections.singleton(other));
    }

    @Override
    public TStream<T> union(Set<TStream<T>> others) {
        if (others.isEmpty())
            return this;
        Set<BOutput> outputs = new HashSet<>();

        // Unwrap all streams so that we do not add the same stream twice
        // in multiple unions or explicitly and in a union.
        for (TStream<T> s : others) {
            StreamImpl<T> si = (StreamImpl<T>) s;
            outputs.add(si.output());
        }

        outputs.add(output());
        if (outputs.size() == 1)
            return this;

        BOutput unionOutput = builder().addUnion(outputs);

        return new StreamImpl<T>(this, unionOutput, getTupleType());
    }

    @Override
    public void print() {
        sink(new Print<T>());
    }

    @Override
    public TStream<T> sample(final double fraction) {
        if (fraction < 0.0 || fraction > 1.0)
            throw new IllegalArgumentException();
        return filter(new RandomSample<T>(fraction));
    }

    @Override
    public TWindow<T,?> last(int count) {
        return new WindowDefinition<T,Object>(this, count);
    }

    @Override
    public TWindow<T,?> window(TWindow<?,?> window) {
        return new WindowDefinition<T,Object>(this, window);
    }

    @Override
    public TWindow<T,?> last(long time, TimeUnit unit) {
        return new WindowDefinition<T,Object>(this, time, unit);
    }

    @Override
    public TWindow<T,?> last() {
        return last(1);
    }
    
    @Override
    public <J, U> TStream<J> join(TWindow<U,?> window,
            BiFunction<T, List<U>, J> joiner) {
        
        Type tupleType = TypeDiscoverer.determineStreamTypeFromFunctionArg(BiFunction.class, 2, joiner);
        
        return ((WindowDefinition<U,?>) window).joinInternal(this, joiner, tupleType);
    }
    
    @Override
    public <J, U> TStream<J> joinLast(TStream<U> other,
            BiFunction<T, U, J> joiner) {
        
        TWindow<U,?> window = other.last();
        
        Type tupleType = TypeDiscoverer.determineStreamTypeFromFunctionArg(BiFunction.class, 2, joiner);
        
        BiFunction<T,List<U>, J> wrapperJoiner = new FirstOfSecondParameterIterator<>(joiner);
        
        return ((WindowDefinition<U,?>) window).joinInternal(this, wrapperJoiner, tupleType);
    }
    

    @Override
    public void publish(String topic) {
        
        if (JSONObject.class.equals(getTupleType())) {
            @SuppressWarnings("unchecked")
            TStream<JSONObject> json = (TStream<JSONObject>) this;
            JSONStreams.toSPL(json).publish(topic);
            return;
        }
        
        
        BOperatorInvocation op;
        if (Schemas.usesDirectSchema(getTupleType())
                 || this instanceof SPLStream) {
            // Publish as a stream consumable by SPL & Java/Scala
            op = builder().addSPLOperator("Publish",
                    "com.ibm.streamsx.topology.topic::Publish",
                    singletonMap("topic", topic));
 
        } else if (getTupleClass() != null){
            // Publish as a stream consumable only by Java/Scala
            Map<String,Object> params = new HashMap<>();
            params.put("topic", topic);
            params.put("class", getTupleClass().getName());
            op = builder().addSPLOperator("Publish",
                    "com.ibm.streamsx.topology.topic::PublishJava",
                    params);
        } else {
            throw new IllegalStateException("A TStream with a tuple type that contains a generic or unknown type cannot be published");
        }

        SourceInfo.setSourceInfo(op, SPL.class);
        this.connectTo(op, false, null);
    }
    
    public TStream<T> parallel(int width, Routing routing) {

        if (width <= 0) {
            throw new IllegalStateException(
                    "The parallel width must be greater"
                            + " than or equal to 1.");
        }

        BOutput toBeParallelized = output();
        boolean needHashRemover = false;
        if (routing == TStream.Routing.HASH_PARTITIONED || routing == TStream.Routing.KEY_PARTITIONED) {

            final ToIntFunction<?> hasher = parallelHasher(routing);
            
            BOperatorInvocation hashAdder = JavaFunctional.addFunctionalOperator(this,
                    "HashAdder",
                    HashAdder.class, hasher);

            BInputPort ip = connectTo(hashAdder, true, null);

            StreamSchema hashSchema = ip.port().getStreamSchema()
                    .extend("int32", "__spl_hash");
            toBeParallelized = hashAdder.addOutput(hashSchema);
            needHashRemover = true;
        }

        BOutput parallelOutput = builder().parallel(toBeParallelized, width);
        if (needHashRemover) {
            parallelOutput.json().put("partitioned", true);
            // Add hash remover
            StreamImpl<T> parallelStream = new StreamImpl<T>(this,
                    parallelOutput, getTupleType());
            BOperatorInvocation hashRemover = builder().addOperator(
                    HashRemover.class, null);
            BInputPort pip = parallelStream.connectTo(hashRemover, true, null);
            parallelOutput = hashRemover.addOutput(pip.port().getStreamSchema()
                    .remove("__spl_hash"));
        }

        return addMatchingStream(parallelOutput);
    }
    
    ToIntFunction<?> parallelHasher(TStream.Routing routing) {
        if (routing == TStream.Routing.HASH_PARTITIONED)
            return ObjectHasher.SINGLETON;
        
        throw new IllegalArgumentException("Routing not supported for this stream:" + routing);
    }

    
    public TStream<T> parallel(int width) {
        return parallel(width, TStream.Routing.ROUND_ROBIN);
    }
    

    public TStream<T> unparallel() {

        // TODO - error checking!
        return addMatchingStream(builder().unparallel(output()));
    }

    @Override
    public TStream<T> throttle(final long delay, final TimeUnit unit) {

        final long delayms = unit.toMillis(delay);

        return modify(new Throttle<T>(delayms));
    }

    /**
     * Connect this stream to a downstream operator. If input is null then a new
     * input port will be created, otherwise it will be used to connect to this
     * stream. Returns input or the new port if input was null.
     */
    @Override
    public BInputPort connectTo(BOperatorInvocation receivingBop, boolean functional,
            BInputPort input) {

        // We go through the JavaFunctional code to ensure
        // that we correctly add the dependent jars into the
        // class path of the operator.
        if (functional)
            return JavaFunctional.connectTo(this, output(), getTupleType(),
                receivingBop, input);
        
        return receivingBop.inputFrom(output, input);
    }

    @Override
    public TStream<T> isolate() {
        BOutput toBeIsolated = output();
        BOutput isolatedOutput = builder().isolate(toBeIsolated); 
        return addMatchingStream(isolatedOutput);
    }
    
    @Override
    public TStream<T> lowLatency() {
        BOutput toBeLowLatency = output();
        BOutput lowLatencyOutput = builder().lowLatency(toBeLowLatency);
        return addMatchingStream(lowLatencyOutput);
    }

    @Override
    public TStream<T> endLowLatency() {
        BOutput toEndLowLatency = output();
        BOutput endedLowLatency = builder().endLowLatency(toEndLowLatency);
        return addMatchingStream(endedLowLatency);

    }

    @Override
    public List<TStream<T>> split(int n, ToIntFunction<T> splitter) {
        if (n <= 0)
            throw new IllegalArgumentException("n");
        
        List<TStream<T>> l = new ArrayList<>(n);
        
        String opName = splitter.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = getTupleName() + "Split";         
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionSplit.class, splitter);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        
        Type outputType = refineType(ToIntFunction.class, 0, splitter);
        for (int i = 0; i < n; i++) {
            TStream<T> splitOutput = JavaFunctional.addJavaOutput(this, bop, outputType);
            l.add(splitOutput);
        }

        return l;
    }
    
    /**
     * Get a stream that is typed to tupleClass,
     * adds a dependency on the type.
     */
    @Override
    public TStream<T> asType(Class<T> tupleClass) {
        if (tupleClass.equals(getTupleClass()))
            return this;
        

        if (output() instanceof BOutputPort) {
            BOutputPort boutput = (BOutputPort) output();
            BOperatorInvocation bop = (BOperatorInvocation) boutput.operator();
        
            return JavaFunctional.getJavaTStream(this, bop, boutput, tupleClass);
        }
        
        // TODO
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <K> TKeyedStream<T, K> key(Function<T, K> keyGetter) {
        if (keyGetter == null)
            throw new NullPointerException();
        return new KeyedStreamImpl<T, K>(this, output,
                refineType(Function.class, 0, keyGetter), keyGetter);
    }
    
    @Override
    public TKeyedStream<T, T> key() {
        return key(new Identity<T>());
    }
    
    @Override
    public boolean isKeyed() {
        return false;
    }
}
