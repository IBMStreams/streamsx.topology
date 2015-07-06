/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static java.util.Collections.singletonMap;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Consumer;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.function7.Predicate;
import com.ibm.streamsx.topology.function7.UnaryOperator;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionFilter;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionMultiTransform;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionSink;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionTransform;
import com.ibm.streamsx.topology.internal.functional.ops.HashRemover;
import com.ibm.streamsx.topology.internal.functional.ops.KeyableTuplePartitioner;
import com.ibm.streamsx.topology.internal.functional.ops.ObjectHashAdder;
import com.ibm.streamsx.topology.internal.logic.Print;
import com.ibm.streamsx.topology.internal.logic.RandomSample;
import com.ibm.streamsx.topology.internal.logic.Throttle;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.tuple.Keyable;

public class StreamImpl<T> extends TupleContainer<T> implements TStream<T> {

    private final BOutput output;

    @Override
    public BOutput output() {
        return output;
    }

    public StreamImpl(TopologyElement te, BOutput output, Class<T> tupleClass) {
        super(te, tupleClass);
        this.output = output;
    }

    @Override
    public TStream<T> filter(Predicate<T> filter) {
        String opName = filter.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = getTupleClass() + "Filter";         
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionFilter.class, filter);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        return JavaFunctional.addJavaOutput(this, bop, getTupleClass());
    }

    @Override
    public void sink(Consumer<T> sinker) {
        
        String opName = sinker.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = getTupleClass().getSimpleName() + "Sink";
        }

        BOperatorInvocation sink = JavaFunctional.addFunctionalOperator(this,
                opName, FunctionSink.class, sinker);
        SourceInfo.setSourceInfo(sink, StreamImpl.class);
        connectTo(sink, true, null);
    }

    @Override
    public <U> TStream<U> transform(Function<T, U> transformer,
            Class<U> tupleTypeClass) {
        
        String opName = transformer.getClass().getSimpleName();
        if (opName.isEmpty()) {
            if (transformer instanceof UnaryOperator)               
                opName = getTupleClass() + "Modify";
            else
                opName = tupleTypeClass.getSimpleName() + "Transform" +
                        getTupleClass().getSimpleName();                
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionTransform.class, transformer);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        return JavaFunctional.addJavaOutput(this, bop, tupleTypeClass);
    }

    @Override
    public TStream<T> modify(UnaryOperator<T> modifier) {
        return transform(modifier, getTupleClass());
    }

    @Override
    public <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer,
            Class<U> tupleTypeClass) {
        
        String opName = transformer.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = tupleTypeClass.getSimpleName() + "MultiTransform" +
                        getTupleClass().getSimpleName();                
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                FunctionMultiTransform.class, transformer);
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        return JavaFunctional.addJavaOutput(this, bop, tupleTypeClass);
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

        return new StreamImpl<T>(this, unionOutput, getTupleClass());
    }

    @Override
    public void print() {
        sink(new Print<T>());
    }

    @Override
    public TStream<T> sample(final double fraction) {
        if (fraction <= 0.0 || fraction >= 1.0)
            throw new IllegalArgumentException();
        return filter(new RandomSample<T>(fraction));
    }

    @Override
    public TWindow<T> last(int count) {
        return new WindowDefinition<T>(this, count);
    }

    @Override
    public TWindow<T> window(TWindow<?> window) {
        return new WindowDefinition<T>(this, window);
    }

    @Override
    public TWindow<T> last(long time, TimeUnit unit) {
        return new WindowDefinition<T>(this, time, unit);
    }

    @Override
    public TWindow<T> last() {
        return last(1);
    }

    @Override
    public <J, U> TStream<J> join(TWindow<U> window,
            BiFunction<T, List<U>, J> joiner, Class<J> tupleClass) {
        return window.join(this, joiner, tupleClass);
    }

    @Override
    public void publish(String topic) {

        BOperatorInvocation op = builder().addSPLOperator("Publish",
                "com.ibm.streamsx.topology.topic::Publish",
                singletonMap("topic", topic));
        SourceInfo.setSourceInfo(op, SPL.class);
        this.connectTo(op, false, null);
    }
    
    public TStream<T> parallel(int width, Routing routing) {

        if (width <= 0) {
            throw new IllegalStateException(
                    "The parallel width must be greater"
                            + " than or equal to 1.");
        }

        // If the type being passed through the Stream is keyable, partition
        // based on
        // the key value of the type. This is accomplished by an operator called
        // KeyableTuplePartitioner which takes the hash value of the type's key
        // and
        // adds it as part of the output stream.
        BOutput toBeParallelized = output();
        if (routing == TStream.Routing.PARTITIONED) {
            BOperatorInvocation hashAdder = null;
            if (Keyable.class.isAssignableFrom(getTupleClass())) {
                hashAdder = builder().addOperator(
                        KeyableTuplePartitioner.class, null);
            } else {
                hashAdder = builder().addOperator(ObjectHashAdder.class, null);
            }

            BInputPort ip = connectTo(hashAdder, true, null);

            StreamSchema hashSchema = ip.port().getStreamSchema()
                    .extend("int32", "__spl_hash");
            toBeParallelized = hashAdder.addOutput(hashSchema);
        }

        BOutput parallelOutput = builder().parallel(toBeParallelized, width);
        if (routing == TStream.Routing.PARTITIONED) {
            parallelOutput.json().put("partitioned", true);
            // Add hash remover
            StreamImpl<T> parallelStream = new StreamImpl<T>(this,
                    parallelOutput, getTupleClass());
            BOperatorInvocation hashRemover = builder().addOperator(
                    HashRemover.class, null);
            BInputPort pip = parallelStream.connectTo(hashRemover, true, null);
            parallelOutput = hashRemover.addOutput(pip.port().getStreamSchema()
                    .remove("__spl_hash"));
        }

        return new StreamImpl<T>(this, parallelOutput, getTupleClass());
    }

    public TStream<T> parallel(int width) {
        if (Keyable.class.isAssignableFrom(getTupleClass())) {
            return parallel(width, TStream.Routing.PARTITIONED);
        } else {
            return parallel(width, TStream.Routing.ROUND_ROBIN);
        }
    }
    

    public TStream<T> unparallel() {

        // TODO - error checking!
        return new StreamImpl<T>(this, builder().unparallel(output()),
                getTupleClass());
    }

    @Override
    public TStream<T> throttle(final long delay, final TimeUnit unit) {

        final long delayms = unit.toMillis(delay);

        return transform(new Throttle<T>(delayms), getTupleClass());
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
            return JavaFunctional.connectTo(this, output(), getTupleClass(),
                receivingBop, input);
        
        return receivingBop.inputFrom(output, input);
    }
}
