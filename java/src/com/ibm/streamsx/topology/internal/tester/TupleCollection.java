/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.tester;

import java.net.InetSocketAddress;
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

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.samples.operators.PassThrough;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.internal.test.handlers.StringTupleTester;
import com.ibm.streamsx.topology.internal.tester.ops.TesterSink;
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
    private OperatorGraph collectorGraph;

    private final Map<TStream<?>, StreamTester> testers = new HashMap<>();

    private final Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers = new HashMap<>();

    public TupleCollection(Topology topology) {
        this.topology = topology;
    }

    /**
     * Holds the information in the declared collector graph about the testers
     * so that the handlers can be attached once the graph is executed.
     */
    private static class StreamTester {
        final int testerId;
        final InputPortDeclaration input;
        final OutputPortDeclaration output;

        // In the graph executing locally, add a PassThrough operator that
        // the TCP server will inject tuples to. It's output will be where
        // the StreamHandlers are attached to.
        StreamTester(OperatorGraph graph, int testerId, TStream<?> stream) {
            this.testerId = testerId;
            OperatorInvocation<PassThrough> operator = graph
                    .addOperator(PassThrough.class);
            input = operator.addInput(stream.output().schema());
            output = operator.addOutput(stream.output().schema());
        }
    }

    private Map<Integer, TestTupleInjector> injectors = Collections
            .synchronizedMap(new HashMap<Integer, TestTupleInjector>());

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
        final StreamCounter<Tuple> counter = new StreamCounter<Tuple>();

        addHandler(stream, counter);

        return new Condition<Long>() {
            
            @Override
            public Long getResult() {
                return counter.getTupleCount();
            }

            @Override
            public boolean valid() {
                return counter.getTupleCount() == expectedCount;
            }

            @Override
            public String toString() {
                return "Expected tuple count: " + expectedCount
                        + " != received: " + counter.getTupleCount();
            }
        };
    }
  
    @Override
    public Condition<Long> atLeastTupleCount(TStream<?> stream, final long expectedCount) {
        final StreamCounter<Tuple> counter = new StreamCounter<Tuple>();

        addHandler(stream, counter);

        return new Condition<Long>() {
            
            @Override
            public Long getResult() {
                return counter.getTupleCount();
            }

            @Override
            public boolean valid() {
                return counter.getTupleCount() >= expectedCount;
            }

            @Override
            public String toString() {
                return "At least tuple count: " + expectedCount
                        + ", received: " + counter.getTupleCount();
            }
        };
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

        final StreamCollector<LinkedList<Tuple>, Tuple> tuples = StreamCollector
                .newLinkedListCollector();

        addHandler(stream, tuples);

        return new Condition<List<Tuple>>() {
            
            @Override
            public List<Tuple> getResult() {
                return tuples.getTuples();
            }

            @Override
            public boolean valid() {
                if (tuples.getTupleCount() != values.length)
                    return false;

                synchronized (tuples) {
                    List<Tuple> sc = tuples.getTuples();
                    for (int i = 0; i < values.length; i++) {
                        if (!sc.get(i).equals(values[i]))
                            return false;
                    }
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

    public void finalizeGraph(StreamsContext.Type contextType,
            Map<String, Object> graphItems) throws Exception {

        if (handlers.isEmpty())
            return;

        switch (contextType) {
        case EMBEDDED_TESTER:
            finalizeEmbeddedTester(graphItems);
            break;
        case BUNDLE:
        case STANDALONE_BUNDLE:
            finalizeStandaloneTester(graphItems);
            break;
        default: // nothing to do
            break;
        }
    }

    private TCPTestServer tcpServer;
    private BOperatorInvocation testerSinkOp;

    // private SPLOperator testerSinkSplOp;

    /**
     * 
     * @param graphItems
     * @throws Exception
     */
    public void finalizeStandaloneTester(Map<String, Object> graphItems)
            throws Exception {

        addTCPServerAndSink();
        collectorGraph = OperatorGraphFactory.newGraph();
        for (TStream<?> stream : handlers.keySet()) {
            int testerId = connectToTesterSink(stream);
            testers.put(stream, new StreamTester(collectorGraph, testerId,
                    stream));
        }

        JavaTestableGraph tg = new JavaOperatorTester()
                .executable(collectorGraph);
        setupTestHandlers(tg, graphItems);
    }

    public void finalizeEmbeddedTester(Map<String, Object> graphItems)
            throws Exception {

    }

    /**
     * Connect a stream in the real topology to the TestSink operator that was
     * added.
     */
    private int connectToTesterSink(TStream<?> stream) {
        BInputPort inputPort = stream.connectTo(testerSinkOp, true, null);
        // testerSinkSplOp.addInput(inputPort);
        return inputPort.port().getPortNumber();
    }

    /**
     * Add a TCP server that will list for tuples to be directed to handlers.
     * Adds a sink to the topology to capture those tuples and deliver them to
     * the current jvm to run Junit type tests.
     */
    private void addTCPServerAndSink() throws Exception {

        tcpServer = new TCPTestServer(0, new IoHandlerAdapter() {
            @Override
            public void messageReceived(IoSession session, Object message)
                    throws Exception {
                TestTuple tuple = (TestTuple) message;
                TestTupleInjector injector = injectors.get(tuple.getTesterId());
                injector.tuple(tuple.getTupleData());
            }
        });

        InetSocketAddress testAddr = tcpServer.start();
        addTesterSink(testAddr);
    }

    public void shutdown() throws Exception {
        tcpServer.shutdown();
    }

    private void addTesterSink(InetSocketAddress testAddr) {

        Map<String, Object> hostInfo = new HashMap<>();
        hostInfo.put("host", testAddr.getHostString());
        hostInfo.put("port", testAddr.getPort());
        this.testerSinkOp = topology.builder().addOperator(TesterSink.class,
                hostInfo);

        /*
         * 
         * testerSinkOp = topology.graph().addOperator(TesterSink.class);
         * 
         * testerSinkSplOp = topology.splgraph().addOperator(testerSinkOp);
         * testerSinkOp.setStringParameter("host", testAddr.getHostString());
         * testerSinkOp.setIntParameter("port", testAddr.getPort());
         * 
         * Map<String, Object> params = new HashMap<>(); params.put("host",
         * testAddr.getHostString()); params.put("port", testAddr.getPort());
         * testerSinkSplOp.setParameters(params);
         */
    }

    private void setupTestHandlers(JavaTestableGraph tg,
            Map<String, Object> graphItems) throws Exception {

        for (TStream<?> stream : handlers.keySet()) {
            Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
            StreamTester tester = testers.get(stream);

            StreamingOutput<OutputTuple> injectPort = tg
                    .getInputTester(tester.input);
            injectors.put(tester.testerId, new TestTupleInjector(injectPort));

            for (StreamHandler<Tuple> streamHandler : streamHandlers) {
                tg.registerStreamHandler(tester.output, streamHandler);
            }
        }

        graphItems.put("testerGraph", tg);
        graphItems.put("testerCollector", this);
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

                // tg.registerStreamHandler(stream.getPort(), streamHandler);
            }
        }
    }
    
    @Override
    public void complete(StreamsContext<?> context) throws Exception {
        if (context.getType() == Type.DISTRIBUTED_TESTER)
            throw new IllegalStateException();
        context.submit(topology).get();
    }
    public boolean complete(StreamsContext<?> context, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        Map<String,Object> noConfig = new HashMap<>(); // Collections.emptyMap();
        return complete(context, noConfig, endCondition,timeout, unit);
    }
    
    public boolean complete(StreamsContext<?> context, Map<String,Object> config, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception {
        
        long totalWait = unit.toMillis(timeout);
        
        if (context.getType() != Type.EMBEDDED_TESTER)
            totalWait += TimeUnit.SECONDS.toMillis(60); // allow extra time for SPL compile
        
        
        final long start = System.currentTimeMillis();
        
        Future<?> future = context.submit(topology, config);
        
        while ((System.currentTimeMillis() - start) < totalWait) {
            long wait = Math.min(1000, totalWait);
            try {
                future.get(wait, TimeUnit.MILLISECONDS);
                break;
            } catch (TimeoutException e) {
                if (endCondition.valid()) {                   
                    break;
                }
            }   
        }
        if (!future.isDone())
            future.cancel(true);
              
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
    
    
}
