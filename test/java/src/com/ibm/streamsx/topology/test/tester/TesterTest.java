/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.tester;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.api.StreamTest;
import com.ibm.streamsx.topology.tester.Condition;

public class TesterTest extends TestTopology {
    @Test
    public void testStringFilter() throws Exception {
        final Topology topology = new Topology("StringFilter");
        TStream<String> source = topology.strings("hello", "goodbye",
                "farewell");
        StreamTest.assertStream(topology, source);

        TStream<String> filtered = source.filter(StreamTest.lengthFilter(5));

        Condition<Long> tupleCount = topology.getTester().tupleCount(filtered, 2);
        Condition<List<String>> contents = topology.getTester().stringContents(filtered,
                "goodbye", "farewell");

        complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);

        assertTrue(tupleCount.valid());
        assertTrue(contents.valid());
    }
}
