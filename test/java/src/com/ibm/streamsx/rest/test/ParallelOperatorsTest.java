/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.rest.Operator;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

public class ParallelOperatorsTest {

    StreamsConnectionTest helper;

    @After
    public void removeJob() throws Exception {
        helper.removeJob();
    }

    @Before
    public void setupJob() throws Exception {
        if (null == helper) {
            helper = new StreamsConnectionTest();
        }
        helper.setupInstance();
        if (helper.jobId == null) {
            // avoid clashes with sub-class tests
            Topology topology = new Topology(getClass().getSimpleName(), 
                    "JobForRESTApiTest");

            TStream<Integer> source = topology.periodicSource(() -> (int) (Math.random() * 5000 + 1), 200, TimeUnit.MILLISECONDS);
            source.invocationName("IntegerPeriodicMultiSource");
            TStream<Integer> parallelSource = source.parallel(3);
            TStream<Integer> sourceDouble = parallelSource.map(StreamsConnectionTest.doubleNumber());
            sourceDouble.invocationName("Outer");
            TStream<Integer> encore = sourceDouble.parallel(2);
            TStream<Integer> encoreDouble = encore.map(StreamsConnectionTest.doubleNumber());
            encoreDouble.invocationName("Inner");
            TStream<Integer> joinDouble = encoreDouble.endParallel();
            TStream<Integer> sourceDoubleAgain = joinDouble.endParallel().isolate().map(StreamsConnectionTest.doubleNumber());
            sourceDoubleAgain.invocationName("ZIntegerTransformInteger");

            if (helper.testType.equals("DISTRIBUTED")) {
                Map<String,Object> cfg = new HashMap<>();
                cfg.put(ContextProperties.STREAMS_CONNECTION, helper.connection);
                helper.jobId = StreamsContextFactory.getStreamsContext(StreamsContext.Type.DISTRIBUTED).submit(topology, cfg).get()
                        .toString();
            } else if (helper.testType.equals("STREAMING_ANALYTICS_SERVICE")) {
                helper.jobId = StreamsContextFactory.getStreamsContext(StreamsContext.Type.STREAMING_ANALYTICS_SERVICE)
                        .submit(topology).get().toString();
            } else {
                fail("This test should be skipped");
            }

            helper.job = helper.instance.getJob(helper.jobId);
            helper.job.waitForHealthy(60, TimeUnit.SECONDS);

            assertEquals("healthy", helper.job.getHealth());
        }
        System.out.println("jobId: " + helper.jobId + " is setup.");
    }

    @Test
    public void testParallelOperators() throws Exception {
        /*
         * Note: the order of the operators is
         * 
         * IntegerPeriodicMultiSource
         * IntegerPeriodicMultiSource_parallel.Outer
         * IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner
         * IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner
         * IntegerPeriodicMultiSource_parallel.Outer
         * IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner
         * IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner
         * IntegerPeriodicMultiSource_parallel.Outer
         * IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner
         * IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner
         * ZIntegerTransformInteger

         * 
         */
        List<Operator> operators = helper.job.getOperators();
        Iterator<Operator> curr = operators.iterator();
        Operator op = null;
        
        assertTrue("Missing source operator", curr.hasNext());
        op = curr.next();
        assertEquals("IntegerPeriodicMultiSource", op.getLogicalName());
        
        for (int outer = 0; outer < 3; ++outer) {
            assertTrue("Missing expected outer parallel operator " + outer, curr.hasNext());
            op = curr.next();
            assertEquals("IntegerPeriodicMultiSource_parallel.Outer", op.getLogicalName());
            assertFalse("Logical name should not match name for outer parallel operator " + outer,
                    op.getName().equals(op.getLogicalName()));

            for (int inner = 0; inner < 2; ++inner) {
                String id = "[" + outer + "," + inner + "]";
                assertTrue("Missing expected inner parallel operator " + id, curr.hasNext());
                op = curr.next();
                assertEquals("IntegerPeriodicMultiSource_parallel.Outer_parallel.Inner", op.getLogicalName());
                assertFalse("Logical name should not match name for inner parallel operator " + id,
                        op.getName().equals(op.getLogicalName()));
                
            }
        }
       
        assertTrue("Missing final operator", curr.hasNext());
        op = curr.next();
        assertEquals("ZIntegerTransformInteger", op.getLogicalName());
    }

}
