/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SPLOperatorsTest extends TestTopology {
    
    /**
     * Test we can invoke an SPL operator.
     */
    @Test
    public void testSPLOperator() throws Exception {
        
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
        Topology topology = new Topology("testSPLOperator"); 
        
        SPLStream tuples = SPLStreamsTest.testTupleStream(topology);
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        params.put("attr", tuples.getSchema().getAttribute("vi"));
        params.put("value", 321);        
   
        SPL.addToolkit(tuples, new File(getTestRoot(), "spl/testtk"));
        SPLStream int32Filtered = SPL.invokeOperator("testspl::Int32Filter", tuples, tuples.getSchema(), params);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(int32Filtered, 2);
        Condition<List<Tuple>> expectedTuples = tester.tupleContents(int32Filtered,
                SPLStreamsTest.TEST_TUPLES[0],
                SPLStreamsTest.TEST_TUPLES[2]
                );

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(expectedTuples.toString(), expectedTuples.valid());
    }
    
    /**
     * Test we can invoke an SPL operator.
     */
    @Test
    public void testLiterals() throws Exception {
        
        // Invokes an SPL operator so cannot run in embedded.       
        assumeSPLOk();   
        
        Topology topology = new Topology("testSPLParameters"); 
        
        StreamSchema schema = Type.Factory.getStreamSchema(
                "tuple<rstring r, int8 i8, int16 i16, int32 i32, int64 i64, float32 f32, float64 f64>");
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        
        Random rand = new Random();
        String r = "test\"Lit\nerals\\n" + rand.nextInt();
        params.put("r", r);
        
        byte i8 = (byte) rand.nextInt();
        short i16 = (short) rand.nextInt(); 
        int i32 = rand.nextInt();
        long i64 = rand.nextLong(); 
        
        
        params.put("i8", i8);
        params.put("i16", i16); 
        params.put("i32", i32); 
        params.put("i64", i64); 
        
        float f32 = rand.nextFloat();
        double f64 = rand.nextDouble();
        params.put("f32", f32); 
        params.put("f64", f64);
   
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        SPLStream paramTuple = SPL.invokeSource(topology, "testgen::TypeLiteralTester", params, schema);

        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(paramTuple, 1);
        MostRecent<Tuple> mr = tester.splHandler(paramTuple, new MostRecent<Tuple>());

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        Tuple tuple = mr.getMostRecentTuple();
        
        assertEquals(r, tuple.getString("r"));
        assertEquals(i8, tuple.getByte("i8"));
        assertEquals(i16, tuple.getShort("i16"));
        assertEquals(i32, tuple.getInt("i32"));
        assertEquals(i64, tuple.getLong("i64"));
        
    }
}
