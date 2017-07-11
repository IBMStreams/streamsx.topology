/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.test.tester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.spl.ExpectedTuples;

public class ConditionTest extends TestTopology {
    @Test
    public void testExactCountGood() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("hello", "goodbye", "farewell");

        Condition<Long> tupleCount = topology.getTester().tupleCount(source, 3);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertTrue(passed);

        assertTrue(tupleCount.valid());
        assertEquals(3L, (long) tupleCount.getResult());
    }
    
    @Test
    public void testExactCountBad1() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("hello", "goodbye", "farewell", "toomuch");

        Condition<Long> tupleCount = topology.getTester().tupleCount(source, 3);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertFalse(passed);      
        assertFalse(tupleCount.valid());
    }
    
    @Test
    public void testExactCountBad2() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("hello", "goodbye");

        Condition<Long> tupleCount = topology.getTester().tupleCount(source, 3);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        assertFalse(passed);
        assertFalse(tupleCount.valid());
    }
    
    @Test
    public void testAtLeastGood() throws Exception {
        final Topology topology = new Topology();
        String[] data = new String[32];
        Arrays.fill(data, "A");
        TStream<String> source = topology.strings(data);

        Condition<Long> tupleCount = topology.getTester().atLeastTupleCount(source, 26);

        boolean passed = complete(topology.getTester(), tupleCount, 10, TimeUnit.SECONDS);
        

        assertTrue(tupleCount.toString(), tupleCount.valid());
        assertTrue(tupleCount.toString(), tupleCount.getResult() >= 26);
        assertTrue(passed);
    }
    
    @Test
    public void testSPLContentsGood() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("A", "B", "C", "D");
        StreamSchema schema = SPLSchemas.STRING.extend("int32", "id");
        SPLStream tested = SPLStreams.convertStream(source, (s,t) -> {t.setString(0, s); t.setInt(1, s.charAt(0)); return t;}, schema);
        
        
        ExpectedTuples expected = new ExpectedTuples(schema);
        int id = "A".charAt(0);
        expected.addAsTuple(new RString("A"), id);
        expected.addAsTuple(new RString("B"), ++id);
        expected.addAsTuple(new RString("C"), ++id);
        expected.addAsTuple(new RString("D"), ++id);

        Condition<List<Tuple>> contents = expected.contents(tested);

        boolean passed = complete(topology.getTester(), contents, 10, TimeUnit.SECONDS);
        assertTrue(contents.toString(), contents.valid());
        assertTrue(passed);
    }
    
    @Test
    public void testSPLContentsBad() throws Exception {
        final Topology topology = new Topology();
        TStream<String> source = topology.strings("A", "B", "C", "D");
        StreamSchema schema = SPLSchemas.STRING.extend("int32", "id");
        SPLStream tested = SPLStreams.convertStream(source, (s,t) -> {t.setString(0, s); t.setInt(1, s.charAt(0)); return t;}, schema);
        
        
        ExpectedTuples expected = new ExpectedTuples(schema);
        int id = "A".charAt(0);
        expected.addAsTuple(new RString("A"), id);
        expected.addAsTuple(new RString("B"), ++id);
        expected.addAsTuple(new RString("C"), 1241241);
        expected.addAsTuple(new RString("D"), ++id);

        Condition<List<Tuple>> contents = expected.contents(tested);

        boolean passed = complete(topology.getTester(), contents, 10, TimeUnit.SECONDS);
        assertFalse(passed);

        assertFalse(contents.toString(), contents.valid());
    }
     
}
