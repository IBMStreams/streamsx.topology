/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.internal.test.handlers.StringTupleTester;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.internal.tester.embedded.EmbeddedTesterRuntime;
import com.ibm.streamsx.topology.internal.tester.tcp.TCPTesterRuntime;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Create a local graph that will collect tuples from the tcp server and connect
 * them to the handlers using this local operator graph, hence reusing the
 * existing infrastructure. The graph will contain a single pass-through
 * operator for any stream under test, the TCP server will inject tuples into
 * the operator and the handlers are connected to the output.
 * 
 */
public class TupleCollection implements Tester {

    private final Topology topology;
    private AtomicBoolean used = new AtomicBoolean();

    private final Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers = new HashMap<>();
    
    private final Map<TStream<?>, Set<UserCondition<?>>> conditions = new HashMap<>();

    public TupleCollection(Topology topology) {
        this.topology = topology;
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
        Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
        if (streamHandlers == null) {
            streamHandlers = new HashSet<StreamHandler<Tuple>>();
            handlers.put(stream, streamHandlers);
        }

        streamHandlers.add(handler);
    }
    private <T> Condition<T> addCondition(TStream<?> stream, UserCondition<T> condition) {
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
        StringTupleTester stt = new StringTupleTester(tester);
        addHandler(stream, stt);
        return stt;
    }
    
    @Override
    public Condition<List<String>> stringContents(TStream<String> stream,
            final String... values) {
        
        stream = stream.asType(String.class);

        final StreamCollector<LinkedList<Tuple>, Tuple> tuples = StreamCollector
                .newLinkedListCollector();

        addHandler(stream, tuples);

        return new Condition<List<String>>() {
            
            @Override
            public List<String> getResult() {
                List<String> strings = new ArrayList<>(tuples.getTupleCount());
                synchronized (tuples.getTuples()) {
                    for (Tuple tuple : tuples.getTuples()) {
                        strings.add(tuple.getString(0));
                    }
                }
                return strings;
            }

            @Override
            public boolean valid() {
                if (tuples.getTupleCount() != values.length)
                    return false;

                List<Tuple> sc = tuples.getTuples();
                for (int i = 0; i < values.length; i++) {
                    if (!sc.get(i).getString(0).equals(values[i]))
                        return false;
                }
                return true;
            }

            @Override
            public String toString() {
                return "Received Tuples: " + getResult();
            }
        };
    }
    
    @Override
    public Condition<List<Tuple>> tupleContents(SPLStream stream,
            final Tuple... values) {
        
        return addCondition(stream, new ContentsUserCondition<>(Arrays.asList(values), true));
    }

    @Override
    public Condition<List<String>> stringContentsUnordered(TStream<String> stream,
            String... values) {

        final List<String> sortedValues = Arrays.asList(values);
        Collections.sort(sortedValues);

        final StreamCollector<LinkedList<Tuple>, Tuple> tuples = StreamCollector
                .newLinkedListCollector();

        addHandler(stream, tuples);

        return new Condition<List<String>>() {
            
            @Override
            public List<String> getResult() {
                List<String> strings = new ArrayList<>(tuples.getTupleCount());
                synchronized (tuples.getTuples()) {
                    for (Tuple tuple : tuples.getTuples()) {
                        strings.add(tuple.getString(0));
                    }
                }
                return strings;
            }

            @Override
            public boolean valid() {

                List<String> strings =  getResult();
                if (strings.size() != sortedValues.size())
                    return false;
                Collections.sort(strings);
                return sortedValues.equals(strings);
            }

            @Override
            public String toString() {
                return "Received Tuples: " + getResult();
            }
        };
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
                runtime = new TCPTesterRuntime(this);
                break;
            default: // nothing to do
                return;
            }
        }
        
        runtime.finalizeTester(handlers, conditions);
    }

    public void setupEmbeddedTestHandlers(JavaTestableGraph tg)
            throws Exception {

        for (TStream<?> stream : handlers.keySet()) {
            Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);

            for (StreamHandler<Tuple> streamHandler : streamHandlers) {
                BOutput output = stream.output();
                if (output instanceof BOutputPort) {
                    BOutputPort outputPort = (BOutputPort) output;
                    tg.registerStreamHandler(outputPort.port(), streamHandler);
                }
            }
        }
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
            totalWait += TimeUnit.SECONDS.toMillis(30); // allow extra time for execution setup              
        
        Future<?> future = context.submit(topology, config);
        
        final long start = System.currentTimeMillis();
        
        boolean endConditionValid = false;
        while ((System.currentTimeMillis() - start) < totalWait) {
            long wait = Math.min(1000, totalWait);
            try {
                future.get(wait, TimeUnit.MILLISECONDS);
                break;
            } catch (TimeoutException e) {
                if (endCondition.valid()) {                   
                    endConditionValid = true;
                    break;
                }
            }   
        }
        if (!future.isDone()) {
            if (!endConditionValid)
                Topology.TOPOLOGY_LOGGER.warning(topology.getName() + " timed out waiting for condition");
            future.cancel(true);
        }
              
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
