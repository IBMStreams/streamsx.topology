/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.python;

import static com.ibm.streamsx.topology.test.splpy.PythonFunctionalOperatorsTest.TUPLE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.test.distributed.PublishSubscribeTest.Delay;
import com.ibm.streamsx.topology.test.splpy.PythonFunctionalOperatorsTest;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class SubscribeSPLDictTest extends PublishSubscribePython {
    
    @Test
    public void testSubscribeMap() throws Exception {
        Topology topology = new Topology("testSubscribeMap");
        
        // Publish a stream with all the SPL types supported by Python.
        SPLStream tuples = PythonFunctionalOperatorsTest.testTupleStream(topology);
        tuples = tuples.modify(new Delay<Tuple>(15));
        tuples.publish("pytest/spl/map");
                     
        SPLStream viaSPL = SPL.invokeOperator("spl.relational::Functor", tuples, tuples.getSchema(), null);
        
        // Python that subscribes to the SPL tuple stream and then republishes as Json.
        includePythonApp(topology, "spl_map_json.py", "spl_map_json::spl_map_json");
           
       
        TStream<JSONObject> viaPythonJson = topology.subscribe("pytest/spl/map/result", JSONObject.class);
                
        SPLStream viaPythonJsonSpl = JSONStreams.toSPL(viaPythonJson.isolate());
        Tester tester = topology.getTester();
        
        Condition<Long> expectedCount = tester.tupleCount(viaPythonJsonSpl, TUPLE_COUNT);
        Condition<Long> expectedCountSpl = tester.tupleCount(viaSPL, TUPLE_COUNT);
               
        Condition<List<Tuple>> viaSPLResult = tester.tupleContents(viaSPL);
        Condition<List<Tuple>> viaPythonResult = tester.tupleContents(viaPythonJsonSpl);
        
        complete(tester, allConditions(expectedCount, expectedCountSpl), 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(expectedCountSpl.valid());
        
        List<Tuple> splResults = viaSPLResult.getResult();
        List<Tuple> pyJsonResults = viaPythonResult.getResult();
                
        assertEquals(TUPLE_COUNT, splResults.size());
        assertEquals(TUPLE_COUNT, pyJsonResults.size());
        
        for (int i = 0; i < TUPLE_COUNT; i++) {
        	Tuple spl = splResults.get(i);
        	JSONObject json = (JSONObject) JSON.parse(pyJsonResults.get(i).getString("jsonString"));
        	
        	System.out.println(spl);
        	System.out.println(pyJsonResults.get(i).getString("jsonString"));
        	
        	assertEquals(spl.getBoolean("b"), ((Boolean) json.get("b")).booleanValue());
        	
        	assertEquals(spl.getInt("i8"), ((Number) json.get("i8")).intValue());
        	assertEquals(spl.getInt("i16"), ((Number) json.get("i16")).intValue());
        	assertEquals(spl.getInt("i32"), ((Number) json.get("i32")).intValue());
        	assertEquals(spl.getLong("i64"), ((Number) json.get("i64")).longValue());
        	
        	assertEquals(spl.getString("r"), json.get("r").toString());
        	
        	assertEquals(spl.getDouble("f32"), ((Number) json.get("f32")).doubleValue(), 0.1);
        	assertEquals(spl.getDouble("f64"), ((Number) json.get("f64")).doubleValue(), 0.1); 
        	
        	{
        		List<?> ex =spl.getList("li32");
            	JSONArray pya = (JSONArray) json.get("li32");
            	assertEquals(ex.size(), pya.size());
            	
            	for (int j = 0; j < ex.size(); j++) {
            		assertEquals(ex.get(j), ((Number) pya.get(j)).intValue());
            	}
        	}

        }
    }
}
