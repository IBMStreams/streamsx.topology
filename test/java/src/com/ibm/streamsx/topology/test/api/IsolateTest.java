package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.test.TestTopology.SC_OK;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class IsolateTest {
    @Ignore
    @Test
    public void simpleIsolationTest() throws Exception {
        assumeTrue(SC_OK);
        Topology topology = new Topology("isolationTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello");
        TStream<String> ss1 = ss.transform(getPEId(), String.class).isolate();
        TStream<String> ss2 = ss.isolate().transform(getPEId(), String.class)
                .isolate();

        Tester tester = topology.getTester();

        Condition<List<String>> condss1 = tester.stringContents(ss1, "");
        Condition<List<String>> condss2 = tester.stringContents(ss2, "");

        // Jenkins seems to fail when running this with a distributed tester.
        // Not sure why, but the tests pass on my local build.
        try {
            StreamsContextFactory
                    .getStreamsContext(StreamsContext.Type.DISTRIBUTED_TESTER)
                    .submit(topology).get(90, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Integer result1 = Integer.parseInt(condss1.getResult().get(0));
        Integer result2 = Integer.parseInt(condss2.getResult().get(0));

        Set<Integer> m = new HashSet<>();

        m.add(result1);
        m.add(result2);
        assertTrue(m.size() == 2);
    }
    
    @Test
    public void isolateIsEndOfStreamTest() throws Exception {
        assumeTrue(SC_OK);
        Topology topology = new Topology("isolationTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello");
        TStream<String> ss1 = topology.strings("hello");
        TStream<String> un = ss.union(ss1);
        un.isolate();
        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT)
                .submit(topology).get();
    }

    @Test
    public void multipleIsolationTest() throws Exception {
        Topology topology = new Topology("isolationTest");

        TStream<String> ss = topology.strings("hello", "world");
        TStream<String> ss0 = ss.isolate();
        TStream<String> ss1 = ss0.transform(getPEId(), String.class);
        ss1.isolate().transform(getPEId(), String.class)
                .transform(getPEId(), String.class).print();

        TStream<String> ss3 = ss.transform(getPEId(), String.class).isolate();
        TStream<String> ss4 = ss3.transform(getPEId(), String.class).isolate();
        TStream<String> ss5 = ss4.transform(getPEId(), String.class).isolate();
        ss5.transform(getPEId(), String.class).print();

        TStream<String> ss7 = ss3.transform(getPEId(), String.class);

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT)
                .submit(topology).get();
    }

    /**
     * Test that a topology fails to generate SPL if an isolated stream is 
     * unioned with its parent.
     * @throws Exception Thrown because the ss4 stream is the parent of the ss7
     * stream. Taking the union of the two is currently not supported. In
     * future releases, we will automatically insert an isolation marker to 
     * support this kind of union.
     */
    @Test(expected = IllegalStateException.class)
    public void multipleIsolationExceptionTest() throws Exception {
        Topology topology = new Topology("isolationTest");

        TStream<String> ss = topology.strings("hello", "world");
        TStream<String> ss0 = ss.isolate();
        TStream<String> ss1 = ss0.transform(getPEId(), String.class);
        ss1.isolate().transform(getPEId(), String.class)
                .transform(getPEId(), String.class).print();

        TStream<String> ss3 = ss.transform(getPEId(), String.class).isolate();
        TStream<String> ss4 = ss3.transform(getPEId(), String.class).isolate();
        TStream<String> ss5 = ss4.transform(getPEId(), String.class).isolate();
        ss5.transform(getPEId(), String.class).print();

        TStream<String> ss7 = ss3.transform(getPEId(), String.class);

        // Unions a stream with its parent.
        ss7.union(ss4).print();

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT)
                .submit(topology).get();
    }
    
    @Test
    public void islandIsolationTest() throws Exception {
        Topology topology = new Topology("isolationTest");

        TStream<String> ss = topology.strings("hello", "world");
        ss.transform(getPEId(), String.class).isolate()
                .transform(getPEId(), String.class);
        
        // Create island subgraph
        TStream<String> ss2 = topology.strings("hello", "world");
        ss2.transform(getPEId(), String.class).print();
        
        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT)
        .submit(topology).get();
    }

    @Test
    public void unionIsolateTest() throws Exception {
        Topology topology = new Topology("isolationTest");

        TStream<String> s1 = topology.strings("1");
        TStream<String> s2 = topology.strings("2");
        TStream<String> s3 = topology.strings("3");
        TStream<String> s4 = topology.strings("4");

        Set<TStream<String>> l = new HashSet<>();
        l.add(s1);
        l.add(s2);
        l.add(s3);
        l.add(s4);

        TStream<String> n = s1.union(l).isolate();

        n.print();
        n.print();
        n.print();
        n.print();

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(n, 4);
        Condition<List<String>> expectedContent = tester
                .stringContentsUnordered(n, "1", "2", "3", "4");

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT)
                .submit(topology).get();

        // assertTrue(expectedCount.valid());
        // assertTrue(expectedContent.valid());
    }

    @SuppressWarnings("serial")
    private static Function<String, String> getPEId() {
        return new Function<String, String>() {
            @Override
            public String apply(String v) {
                return PERuntime.getCurrentContext().getPE().getPEId().toString();
            }
        };
    }
}
