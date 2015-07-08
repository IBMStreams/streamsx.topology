package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.test.TestTopology.SC_OK;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class IsolateTest {
    
    @Test
    public void simpleIsolationTest() throws Exception{
        assumeTrue(SC_OK);
        Topology topology = new Topology("isolationTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello");
        TStream<String> ss1 = ss.transform(getPEId(), String.class).isolate();
        TStream<String> ss2 = ss.isolate().transform(getPEId(), String.class).isolate();
        
        Tester tester = topology.getTester();
        
        Condition<List<String>> condss1 = tester.stringContents(ss1, "");      
        Condition<List<String>> condss2 = tester.stringContents(ss2, "");
        
        try{
        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.DISTRIBUTED_TESTER)
                .submit(topology).get(90, TimeUnit.SECONDS);
        } catch(Exception e){
            e.printStackTrace();
        }
        Integer result1 = Integer.parseInt(condss1.getResult().get(0));
        Integer result2 = Integer.parseInt(condss2.getResult().get(0));
        
        Set<Integer> m = new HashSet<>();

        m.add(result1);
        m.add(result2);
        assertTrue(m.size() == 2);
    }

    @Test(expected = IllegalStateException.class)
    public void multipleIsolationExceptionTest() throws Exception{
        Topology topology = new Topology("isolationTest");

        TStream<String> ss = topology.strings("hello", "world");
        TStream<String> ss0 = ss.isolate();
        TStream<String> ss1 = ss0.transform(getPEId(), String.class);
        ss1.isolate().transform(getPEId(), String.class).transform(getPEId(), String.class).print();

        
        TStream<String> ss3 = ss.transform(getPEId(), String.class).isolate();
        TStream<String> ss4 = ss3.transform(getPEId(), String.class).isolate();
        TStream<String> ss5 = ss4.transform(getPEId(), String.class).isolate();
        ss5.transform(getPEId(), String.class).print();

        TStream<String> ss7 = ss3.transform(getPEId(), String.class);

        ss7.union(ss4).print();

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT).submit(topology).get();
    }
    
    @Test
    public void unionIsolateTest() throws Exception{
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
        Condition<List<String>> expectedContent = tester.stringContentsUnordered(n, "1", "2", "3", "4");

        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();

        assertTrue(expectedCount.valid());
        assertTrue(expectedContent.valid());
    }
 
    @SuppressWarnings("serial")
    private static Function<String, String> getPEId(){
        return new Function<String, String>(){
            int counter = 0;
            @Override
                public String apply(String v) {
                // TODO Auto-generated method stub
                return ((BigInteger) PERuntime.getCurrentContext().getPE().getPEId()).toString();
            }

        };
    }
}
