/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static com.ibm.streamsx.topology.test.splpy.PythonFunctionalOperatorsTest.TEST_TUPLES;
import static com.ibm.streamsx.topology.test.splpy.PythonFunctionalOperatorsTest.sampleFilterStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class PythonFunctionalOperatorsKwargsTest extends TestTopology {
	
    @Before
    public void runSpl() {
        assumeSPLOk();
        
        assumeTrue(getTesterContext().getType() == StreamsContext.Type.STANDALONE_TESTER
        		|| getTesterContext().getType() == StreamsContext.Type.DISTRIBUTED_TESTER
        		|| isStreamingAnalyticsRun());
    }
    
    @Test
    public void testFilter() throws Exception {
        Topology topology = new Topology("testFilter");
        
        SPLStream tuples = sampleFilterStream(topology);
        
        PythonFunctionalOperatorsTest.addTestToolkit(tuples);
        SPLStream pass = SPL.invokeOperator("com.ibm.streamsx.topology.pysamples.kwargs::ContainsFilter",
        		tuples, tuples.getSchema(), Collections.singletonMap("term", "23"));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(pass, 2);
        
        Condition<List<Tuple>> passResult = tester.tupleContents(pass);

        this.getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);
     
        assertEquals(TEST_TUPLES[1], passResult.getResult().get(0));
        assertEquals(TEST_TUPLES[3], passResult.getResult().get(1));
        assertTrue(expectedCount.valid());
    }
    @Test
    public void testFilterOptionalOutput() throws Exception {
        Topology topology = new Topology("testFilterOptionalOutput");
        
        SPLStream tuples = sampleFilterStream(topology);
        
        PythonFunctionalOperatorsTest.addTestToolkit(tuples);
        List<SPLStream> filtered = SPL.invokeOperator(topology, "CFOpt",
        		"com.ibm.streamsx.topology.pysamples.kwargs::ContainsFilter",
        		Collections.singletonList(tuples),
        		Collections.nCopies(2, tuples.getSchema()),
        		Collections.singletonMap("term", "23"));
        
        SPLStream pass = filtered.get(0);
        SPLStream failed = filtered.get(1);

        Tester tester = topology.getTester();
        Condition<Long> expectedPassCount = tester.tupleCount(pass, 2);
        Condition<Long> expectedFailedCount = tester.tupleCount(failed, 2);
               
        Condition<List<Tuple>> passResult = tester.tupleContents(pass);
        Condition<List<Tuple>> failedResult = tester.tupleContents(failed);

        getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        complete(tester, expectedPassCount.and(expectedFailedCount), 10, TimeUnit.SECONDS);
     
        assertEquals(TEST_TUPLES[1], passResult.getResult().get(0));
        assertEquals(TEST_TUPLES[3], passResult.getResult().get(1));
        
        assertEquals(TEST_TUPLES[0], failedResult.getResult().get(0));
        assertEquals(TEST_TUPLES[2], failedResult.getResult().get(1));
    }
    @Test
    public void testMap() throws Exception {
        Topology topology = new Topology("testMap");
                
        SPLStream tuples = sampleFilterStream(topology);
        
        PythonFunctionalOperatorsTest.addTestToolkit(tuples);
        SPLStream mapped = SPL.invokeOperator("Mp",
        		"com.ibm.streamsx.topology.pytest.pymap::OffByOne",
        		tuples,
        		tuples.getSchema(), null);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(mapped, 3);
               
        Condition<List<Tuple>> result = tester.tupleContents(mapped, TEST_TUPLES[0], TEST_TUPLES[1], TEST_TUPLES[2]);

        getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);
     
        assertTrue(result.getResult().toString(), result.valid());
    }
}
