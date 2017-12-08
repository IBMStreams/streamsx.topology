/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.spl.SPLStreamsTest;

/**
 * Test publish/subscribe. These tests just use publish/subscribe
 * within a single job, but the expected use case is across jobs.
 *
 */
public class PublishSubscribeSPLTest extends TestTopology {

    @Before
    public void checkIsDistributed() {
        // Uses the SPL part of the topology api
        assumeTrue(hasStreamsInstall());
        
        assumeTrue(isDistributedOrService());
    }

    /**
     * Test that with publish allow filter set to false
     * a subscriber without a filter gets the full set of data.
     */
    @Test
    public void testSPLPublishNoFilterWithSubscribe() throws Exception {
        final Topology t = new Topology();
        
        SPLStream source = SPLStreamsTest.testTupleStream(t);
                
        source = addStartupDelay(source);
        
        final String topic = "testSPLPublishNoFilterSFilteredSubscribe/" + System.currentTimeMillis();
        
        source.publish(topic, false);
        
        SPLStream sub = SPLStreams.subscribe(t, topic, source.getSchema());
        
        TStream<String> subscribe = sub.transform(new GetTupleId());

        completeAndValidate(subscribe, 20, "SPL:0", "SPL:1", "SPL:2", "SPL:3");
    } 
    @Test
    public void testSPLPublishNoFilterWithSubscribeSubmissionParams() throws Exception {
        final Topology t = new Topology();
        
        SPLStream source = SPLStreamsTest.testTupleStream(t);
                
        source = addStartupDelay(source);
        
        final String topic = "testSPLPublishNoFilterSFilteredSubscribe/" + System.currentTimeMillis();
        
        Supplier<String> pubParam = t.createSubmissionParameter("PPSPL", String.class);
        source.publish(pubParam);
        
        Supplier<String> subParam = t.createSubmissionParameter("SPSPL", String.class);
        SPLStream sub = SPLStreams.subscribe(t, subParam, source.getSchema());
        
        TStream<String> subscribe = sub.transform(new GetTupleId());
        
        JobConfig jco = new JobConfig();
        jco.addSubmissionParameter("SPSPL", topic); 
        jco.addSubmissionParameter("PPSPL", topic); 
        jco.addToConfig(getConfig());

        completeAndValidate(subscribe, 20, "SPL:0", "SPL:1", "SPL:2", "SPL:3");
    } 
    /**
     * Test that with publish allow filter set to false
     * a subscriber without a filter gets the full set of data.
     */
    @Test
    public void testSPLPublishAllowFilterWithSubscribe() throws Exception {
        final Topology t = new Topology();
        
        SPLStream source = SPLStreamsTest.testTupleStream(t);
        
        final String topic = "testSPLPublishAllowFilterWithSubscribe/" + System.currentTimeMillis();
                
        source = addStartupDelay(source);
        
        source.publish(topic, true);
        
        SPLStream sub = SPLStreams.subscribe(t, topic, source.getSchema());
        
        TStream<String> subscribe = sub.transform(new GetTupleId());

        completeAndValidate(subscribe, 20, "SPL:0", "SPL:1", "SPL:2", "SPL:3");
    }

    public static class GetTupleId implements Function<Tuple, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(Tuple v) {
            return "SPL:" + v.getString("id");
        }
    }

    @Test
    public void testSPLPublishAllowFilterWithFilteredSubscribe() throws Exception {
        _testSPLPublishFilteredSubscribe(
              "testSPLPublishAllowFilterWithFilteredSubscribe", true, true);
  }
    @Test
    public void testSPLPublishNoFilterWithFilteredSubscribe() throws Exception {
        _testSPLPublishFilteredSubscribe(
              "testSPLPublishNoFilterWithFilteredSubscribe", false, false);
  }
  private void _testSPLPublishFilteredSubscribe(String topic, boolean allowFilters, boolean useParams) throws Exception {
        final Topology t = new Topology();
        
        topic = topic + "/" + System.currentTimeMillis();
        
        SPL.addToolkit(t, new File(getTestRoot(), "spl/testtk"));
        
        SPLStream source = SPLStreamsTest.testTupleStream(t);
                
        source = addStartupDelay(source);
        
        Supplier<String> pubParam = useParams ?
                t.createSubmissionParameter("PFSPL", String.class) : null;
        if (useParams)
            source.publish(pubParam, allowFilters);
        else
            source.publish(topic, allowFilters);
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        params.put("topic", topic);
        params.put("value", 43675232L);        
  
        SPLStream sub = SPL.invokeSource(t, "testspl::Int64SubscribeFilter", params, source.getSchema());
        
        TStream<String> subscribe = sub.transform(new GetTupleId());
        
        if (useParams) {
            JobConfig jco = new JobConfig();
            jco.addSubmissionParameter("PFSPL", topic); 
            jco.addToConfig(getConfig());
        }

        completeAndValidate(subscribe, 20, "SPL:1");
    } 
}
