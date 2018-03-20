/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TStream.Routing;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SPLParallelTest extends TestTopology {
    
    @Before
    public void checkOK() {
        assumeSPLOk();
    }

    @Test
    public void testRoundRobin() throws Exception {

        Topology topology = newTopology();

        TStream<String> values = topology.strings("UDP-RR", "@parallel-RR", "Done-RR");
        StreamSchema schema = com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<rstring x>");
        SPLStream vs = SPLStreams.convertStream(values,
                (s,o) -> {o.setString(0, s); return o;}, schema);
        
        vs = vs.parallel(()->3, Routing.ROUND_ROBIN);
        SPLStream r = vs.filter(new AllowAll<>()).endParallel();
        TStream<String> rs = SPLStreams.toStringStream(r);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(r, 3);
        Condition<List<String>> content = tester.stringContentsUnordered(rs, "UDP-RR", "@parallel-RR", "Done-RR");
        assertTrue(complete(tester, expectedCount, 20, TimeUnit.SECONDS));
        assertTrue(content.valid());
    }
    
    @Test
    public void testBroadcast() throws Exception {

        Topology topology = newTopology();

        TStream<String> values = topology.strings("UDP-B", "@parallel-B", "Done-B");
        StreamSchema schema = com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<rstring x>");
        SPLStream vs = SPLStreams.convertStream(values,
                (s,o) -> {o.setString(0, s); return o;}, schema);
        
        vs = vs.parallel(()->3, Routing.BROADCAST);
        SPLStream r = vs.filter(new AllowAll<>()).endParallel();
        TStream<String> rs = SPLStreams.toStringStream(r);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(r, 9);
        Condition<List<String>> content = tester.stringContentsUnordered(rs,
                "UDP-B", "@parallel-B", "Done-B", "UDP-B", "@parallel-B", "Done-B", "UDP-B", "@parallel-B", "Done-B");
        assertTrue(complete(tester, expectedCount, 20, TimeUnit.SECONDS));
        assertTrue(content.valid());
    }
    
    @Test(expected=NullPointerException.class)
    public void testNullRouting() throws Exception {
        unsupportedRouting(null);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testHashPartitioned() throws Exception {
        unsupportedRouting(Routing.HASH_PARTITIONED); 
    }
    @Test(expected=IllegalArgumentException.class)
    public void testKeyPartitioned() throws Exception {
        unsupportedRouting(Routing.KEY_PARTITIONED); 
    }
    
    private void unsupportedRouting(Routing routing) {

        Topology topology = newTopology();
        TStream<String> a = topology.strings("A");
        SPLStream as = SPLStreams.stringToSPLStream(a);        
        as = as.parallel(()->3, routing); 
    }
    

}
