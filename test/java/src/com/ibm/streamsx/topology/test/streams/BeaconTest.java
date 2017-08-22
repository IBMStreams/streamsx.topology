/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
public class BeaconTest extends TestTopology {
    
    @Test
    public void testBeaconTupleToString() throws Exception {
        assumeTrue(isMainRun());
        BeaconTuple bt1 = new BeaconTuple(0, 1);
        BeaconTuple bt2 = new BeaconTuple(0, 2);
        
        String s = bt1.toString();
        assertNotNull(s);
        assertFalse(s.isEmpty());
        assertFalse(s.equals(bt2.toString()));
    }
    
    @Test
    public void testBeaconTupleHashCode() throws Exception {
        assumeTrue(isMainRun());
        BeaconTuple bt1 = new BeaconTuple(0, 1);
        BeaconTuple bt2 = new BeaconTuple(0, 2);
        BeaconTuple bt3 = new BeaconTuple(0, 2);
        
        assertTrue(bt1.hashCode() != 0);
        assertTrue(bt1.hashCode() != bt2.hashCode());
        assertTrue(bt2.hashCode() == bt3.hashCode());
    }
    
    @Test
    public void testBeaconTupleEquals() throws Exception {
        assumeTrue(isMainRun());
        BeaconTuple bt1 = new BeaconTuple(0, 1);
        BeaconTuple bt2 = new BeaconTuple(0, 2);
        BeaconTuple bt3 = new BeaconTuple(0, 2);
        BeaconTuple bt4 = new BeaconTuple(1, 2);
        
        assertTrue(!bt1.equals("foo"));
        assertTrue(!bt1.equals(null));
        assertTrue(bt1.equals(bt1));
        assertTrue(!bt1.equals(bt2));
        assertTrue(bt2.equals(bt3));
        assertTrue(!bt3.equals(bt4));
    }

    @Test
    public void testFixedCount() throws Exception {
        Topology topology = newTopology("testFixedCount");

        final int count = new Random().nextInt(1000) + 37;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(fb, count);

        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
    }

    @Test
    public void testLongFixedCount() throws Exception {
        Topology topology = newTopology("testLongFixedCount");

        final int count = 7;

        TStream<Long> fb = BeaconStreams.longBeacon(topology, count);
        TStream<String> fs = StringStreams.toString(fb);
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(fs, count);
        Condition<List<String>> expectedContents = tester.stringContents(fs, "0", "1", "2",
                "3", "4", "5", "6");
        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(expectedContents.valid());
    }
    
    @Test
    public void testForeverBeacon() throws Exception {
        Topology topology = newTopology("testForeverBeacon");

        final int count = new Random().nextInt(1000) + 37;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);

        Tester tester = topology.getTester();
        Condition<Long> endCondition = tester.atLeastTupleCount(fb, count);

        assertTrue(complete(tester, endCondition, 20, TimeUnit.SECONDS));
    }
    
    @Test
    public void testBeaconTuples() throws Exception {
        
        // Uses IBM JSON4J
        assumeTrue(hasStreamsInstall());       

        final int count = new Random().nextInt(1000) + 37;

        Topology topology = newTopology();
        TStream<BeaconTuple> beacon = BeaconStreams.beacon(topology, count);
        TStream<JSONObject> json = JSONStreams.toJSON(beacon);
        TStream<String> strings = JSONStreams.serialize(json);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(strings, count);
        
        Condition<String> contents = createPredicate(strings, tester);

        complete(tester, expectedCount, 20, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(contents.toString(), contents.valid());
    }

    private static Condition<String> createPredicate(TStream<String> strings, Tester tester) {
        @SuppressWarnings("serial")
        Condition<String> contents = tester.stringTupleTester(strings, new Predicate<String>() {
            
            private transient BeaconTuple lastTuple;

            @Override
            public boolean test(String tuple) {
                
                JSONObject json;
                try {
                    json = (JSONObject) JSON.parse(tuple);
                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                BeaconTuple bt = new BeaconTuple((Long) json.get("sequence"), (Long) json.get("time"));
                boolean ok;
                if (lastTuple == null) {
                    ok = bt.getSequence() == 0;
                } else {
                    ok = lastTuple.compareTo(bt) < 0
                         && lastTuple.getTime() <= bt.getTime()
                         && lastTuple.getSequence() + 1 == bt.getSequence();
                            
                }
                
                ok = ok && bt.getTime() != 0;
                ok = ok && bt.getKey() == bt.getSequence();
                                      
                lastTuple = bt;
                return ok;
            }});
        return contents;
    }
    
    @Test
    public void testBeaconTypes() {
        assumeTrue(isMainRun());
        Topology t = newTopology();
        assertEquals(BeaconTuple.class, BeaconStreams.beacon(t).getTupleClass());
        assertEquals(BeaconTuple.class, BeaconStreams.beacon(t, 77).getTupleClass());
        
        assertEquals(Long.class, BeaconStreams.longBeacon(t).getTupleClass());
        assertEquals(Long.class, BeaconStreams.longBeacon(t, 23).getTupleClass());
        
        assertEquals(Long.class, BeaconStreams.single(t).getTupleClass());
    }
}
