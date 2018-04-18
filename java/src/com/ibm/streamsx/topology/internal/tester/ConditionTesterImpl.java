/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester;

import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.NO_PROGRESS;
import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.PROGRESS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.ResetterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.StringPredicateUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.internal.tester.embedded.EmbeddedTesterRuntime;
import com.ibm.streamsx.topology.internal.tester.rest.RESTTesterRuntime;
import com.ibm.streamsx.topology.internal.tester.tcp.TCPTesterRuntime;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Collects a set of conditions against a topology
 * and then allows a TesterRuntime to implement
 * them against a topology.
 */
public class ConditionTesterImpl implements Tester {

    private final Topology topology;
    private AtomicBoolean used = new AtomicBoolean();

    private final Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers = new HashMap<>();
    
    private final Map<TStream<?>, Set<UserCondition<?>>> conditions = new HashMap<>();

    public ConditionTesterImpl(Topology topology) {
        this.topology = topology;
    }
    
    public boolean hasTests() {
        return !handlers.isEmpty() || !conditions.isEmpty();
    }

    /*
     * Graph declaration time.
     */

    /**
     * Just collect the stream handlers that may be used for testing of this
     * topoogy. What gets added to the graph depends on the context it is run
     * in.
     */
    private void addHandler(TStream<?> stream, StreamHandler<Tuple> handler) {
        checkStream(stream);
        Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
        if (streamHandlers == null) {
            streamHandlers = new HashSet<StreamHandler<Tuple>>();
            handlers.put(stream, streamHandlers);
        }

        streamHandlers.add(handler);
    }
    
    private void checkStream(TStream<?> stream) {
        // Some conditions are not against streams.
        if (stream == null)
            return;
        if (stream.topology() != this.getTopology())
            throw new IllegalStateException();
    }
    
    
    private <T> Condition<T> addCondition(TStream<?> stream, UserCondition<T> condition) {
        checkStream(stream);
        Set<UserCondition<?>> streamConditions = conditions.get(stream);
        if (streamConditions == null) {
            streamConditions = new HashSet<>();
            conditions.put(stream, streamConditions);
        }

        streamConditions.add(condition);
        return condition;
    }

    @Override
    public <T extends StreamHandler<Tuple>> T splHandler(SPLStream stream,
            T handler) {
        // initialize();
        // addTester(stream, handler);

        addHandler(stream, handler);
        return handler;
    }
    
    

    @Override
    public Condition<Long> tupleCount(TStream<?> stream, final long expectedCount) {
        
        return addCondition(stream, new CounterUserCondition(expectedCount, true));
    }
  
    @Override
    public Condition<Long> atLeastTupleCount(TStream<?> stream, final long expectedCount) {       
        return addCondition(stream, new CounterUserCondition(expectedCount, false));
    }
    
    @Override
    public Topology getTopology() {
        return topology;
    }
    @Override
    public Condition<String> stringTupleTester(TStream<String> stream,
            Predicate<String> tester) {
        
        return addCondition(stream, new StringPredicateUserCondition(tester));
    }
    
    @Override
    public Condition<List<String>> stringContents(TStream<String> stream,
            final String... values) {
        
        stream = stream.asType(String.class);
        
        return addCondition(stream, new ContentsUserCondition<String>(String.class, Arrays.asList(values), true));
    }
    
    @Override
    public Condition<List<Tuple>> tupleContents(SPLStream stream,
            final Tuple... values) {
        
        return addCondition(stream, new ContentsUserCondition<>(Tuple.class, Arrays.asList(values), true));
    }

    @Override
    public Condition<List<String>> stringContentsUnordered(TStream<String> stream,
            String... values) {
        
        return addCondition(stream, new ContentsUserCondition<String>(String.class, Arrays.asList(values), false));
    }
    
    @Override
    public Condition<Void> resetConsistentRegions(Integer minimumResets) {
        if (minimumResets != null && minimumResets <= 0) {
            throw new IllegalArgumentException(minimumResets.toString());
        }
        return addCondition(null, new ResetterUserCondition(minimumResets));
    }

    /*
     * Graph finalization time.
     */

    public void finalizeGraph(StreamsContext.Type contextType) throws Exception {
                
        if (handlers.isEmpty() && conditions.isEmpty())
            return;

        synchronized (this) {
            switch (contextType) {
            case EMBEDDED_TESTER:
                runtime = new EmbeddedTesterRuntime(this);
                break;
            case DISTRIBUTED_TESTER:
            case STANDALONE_TESTER:
                runtime = new TCPTesterRuntime(contextType, this);
                break;
            case STREAMING_ANALYTICS_SERVICE_TESTER:
                runtime = new RESTTesterRuntime(this);
                break;
            default: // nothing to do
                return;
            }
        }
        
        runtime.finalizeTester(handlers, conditions);
    }
    
    private void checkOneUse() {
        if (!used.compareAndSet(false, true))
            throw new IllegalStateException("One use only");
    }
    
    @Override
    public void complete(StreamsContext<?> context) throws Exception {
        if (context.getType() == Type.DISTRIBUTED_TESTER)
            throw new IllegalStateException();
        checkOneUse();
        context.submit(topology).get();
    }
    @Override
    public boolean complete(StreamsContext<?> context, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        Map<String,Object> noConfig = new HashMap<>(); // Collections.emptyMap();
        return complete(context, noConfig, endCondition,timeout, unit);
    }
    @Override
    public boolean complete(StreamsContext<?> context, Map<String,Object> config, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        checkOneUse();
        
        long totalWait = unit.toMillis(timeout);
        
        if (context.getType() != Type.EMBEDDED_TESTER)
            totalWait += SECONDS.toMillis(30); // allow extra time for execution setup              
        
        Future<?> future = context.submit(topology, config);
        
        final long start = System.currentTimeMillis();
        
        TestState state = null;
        boolean seenValid = false;
        while (state == null || (System.currentTimeMillis() - start) < totalWait) {
            
            state = getRuntime().checkTestState(context, config, future, endCondition);
            switch (state) {
            case NOT_READY:
                continue;
            case NO_PROGRESS:
                if (seenValid)
                    break;
                continue;
            case PROGRESS:
                if (seenValid)
                    break;
                // delay a timeout since progress is being made
                totalWait += SECONDS.toMillis(1);
                continue;
                
            case FAIL:
                break;
                
            case VALID:
                if (seenValid)
                    break;
                seenValid = true;
                // Add one additional check to allow the
                // condition to become invalid, e.g. expecting 10 tuples
                // becomes valid but then another tuple arrives
                state = null;
                continue;

            }
            break;
        }
              
        if (state == null || state == NO_PROGRESS || state == PROGRESS) {
            Topology.TOPOLOGY_LOGGER.warning(topology.getName() + " timed out waiting for condition");           
        }
        
        getRuntime().shutdown(future);
              
        return endCondition.valid();
    }
    // 
    public Condition<List<String>> completeAndTestStringOutput(StreamsContext<?> context,
            TStream<?> output, long timeout, TimeUnit unit, String... contents)
            throws Exception {
        
        Map<String,Object> noConfig = new HashMap<>(); // Collections.emptyMap();
        return completeAndTestStringOutput(context, noConfig, output, timeout, unit, contents);

    }
    @SuppressWarnings("unchecked")
    @Override
    public Condition<List<String>> completeAndTestStringOutput(StreamsContext<?> context,
            Map<String,Object> config,
            TStream<?> output, long timeout, TimeUnit unit, String... contents)
            throws Exception {
        
        if (output.topology() != topology)
            throw new IllegalArgumentException();
        
        TStream<String> stringOutput;
        if (String.class.equals(output.getTupleClass()))
            stringOutput = (TStream<String>) output;
        else
            stringOutput = StringStreams.toString(output);
        
        Condition<Long> expectedCount = tupleCount(stringOutput, contents.length);
        Condition<List<String>> expectedContents = stringContents(stringOutput, contents);

        complete(context, config, expectedCount, timeout, unit);
        return expectedContents;
    }
    
    private TesterRuntime runtime;
    public synchronized TesterRuntime getRuntime() {
        if (runtime == null)
            throw new IllegalStateException();
        return runtime;
    }
}
