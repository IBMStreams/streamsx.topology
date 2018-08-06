/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.tcp;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.FAIL;
import static com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState.NO_PROGRESS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.samples.operators.PassThrough;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.internal.streams.InvokeCancel;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.handlers.HandlerTesterRuntime;
import com.ibm.streamsx.topology.internal.tester.ops.TesterSink;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Create a local graph that will collect tuples from the tcp server and connect
 * them to the handlers using this local operator graph, hence reusing the
 * existing infrastructure. The graph will contain a single pass-through
 * operator for any stream under test, the TCP server will inject tuples into
 * the operator and the handlers are connected to the output.
 */
public class TCPTesterRuntime extends HandlerTesterRuntime {
    
    private OperatorGraph collectorGraph;
    private JavaTestableGraph localCollector;
    private Future<JavaTestableGraph> localRunningCollector;
    private TCPTestServer tcpServer;
    private BOperatorInvocation testerSinkOp;
    private final Map<TStream<?>, StreamTester> testers = new HashMap<>();
    private final StreamsContext.Type contextType;
    
    private Map<Integer, TestTupleInjector> injectors = Collections
            .synchronizedMap(new HashMap<Integer, TestTupleInjector>());


    public TCPTesterRuntime(StreamsContext.Type contextType, ConditionTesterImpl tester) {
        super(tester);
        this.contextType = contextType;
    }

    /**
     * 
     * @param graphItems
     * @throws Exception
     */
    public void finalizeTester(Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions)
            throws Exception {
        
        super.finalizeTester(handlers, conditions);

        addTCPServerAndSink();
        collectorGraph = OperatorGraphFactory.newGraph();
        for (TStream<?> stream : this.handlers.keySet()) {
            if (stream == null)
                continue;
            int testerId = connectToTesterSink(stream);
            testers.put(stream, new StreamTester(collectorGraph, testerId,
                    stream));
        }       

        localCollector = new JavaOperatorTester()
                .executable(collectorGraph);
        
        setupTestHandlers();
    }
    
    @Override
    public void start(Object info) {
        assert this.localCollector != null;
        localRunningCollector = localCollector.execute();
    }   
    
    /**
     * Add a TCP server that will list for tuples to be directed to handlers.
     * Adds a sink to the topology to capture those tuples and deliver them to
     * the current jvm to run Junit type tests.
     */
    private void addTCPServerAndSink() throws Exception {

        tcpServer = new TCPTestServer(0,
                this.contextType == StreamsContext.Type.STANDALONE_TESTER,
                new IoHandlerAdapter() {
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

    @Override
    public void shutdown(Future<?> future) throws Exception {
        try {
            if (contextType == Type.DISTRIBUTED_TESTER) {
                InvokeCancel cancel = new InvokeCancel((BigInteger) (future.get()));
                cancel.invoke(false);
            }
        } finally {
            try { tcpServer.shutdown(); }
                 finally {
                 try { localRunningCollector.cancel(true); }
                      finally {future.cancel(true);}
            }   
        }
    }

    private void addTesterSink(InetSocketAddress testAddr) {      
        Map<String, Object> hostInfo = new HashMap<>();
        hostInfo.put("host", testAddr.getHostString());
        hostInfo.put("port", testAddr.getPort());        
        this.testerSinkOp = topology().builder().addOperator(
                "TesterTCP" + testAddr.getPort(),
                TesterSink.KIND,
                hostInfo);
        this.testerSinkOp.setModel(MODEL_SPL, LANGUAGE_JAVA);
    }
    
    /**
     * Connect a stream in the real topology to the TestSink operator that was
     * added.
     */
    private int connectToTesterSink(TStream<?> stream) {
        BInputPort inputPort = stream.connectTo(testerSinkOp, true, null);
        return inputPort.index();
    }



    private void setupTestHandlers() throws Exception {

        for (TStream<?> stream : handlers.keySet()) {
            if (stream == null)
                continue;
            
            Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
            StreamTester tester = testers.get(stream);

            StreamingOutput<OutputTuple> injectPort = localCollector
                    .getInputTester(tester.input);
            injectors.put(tester.testerId, new TestTupleInjector(injectPort));

            for (StreamHandler<Tuple> streamHandler : streamHandlers) {
                localCollector.registerStreamHandler(tester.output, streamHandler);
            }
        }
    }
    
    @Override
    public TestState checkTestState(StreamsContext<?> context, Map<String, Object> config, Future<?> future,
            Condition<?> endCondition) throws Exception {
        
        TestState state;
        if (context.getType() == Type.STANDALONE_TESTER) {
            state = checkStandaloneTestState(future, endCondition);
        } else {
            SECONDS.sleep(1);
            state = this.testStateFromConditions(false, true);
        }
                   
        return state;
    }

    private TestState checkStandaloneTestState(Future<?> future, Condition<?> endCondition) throws Exception {
        try {
            int rc = (Integer) future.get(1, SECONDS);
            if (rc != 0)
                return FAIL;
            return this.testStateFromConditions(true, true);
        } catch (TimeoutException e) {
            if (endCondition.valid()) {
                return testStateFromConditions(true, true);
            }
            return NO_PROGRESS;
        }
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
            input = operator.addInput(stream.output()._type());
            output = operator.addOutput(stream.output()._type());
        }
    }
}
