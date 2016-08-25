/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
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
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Function;
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
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */

    @Before
    public void checkIsDistributed() {
        assumeTrue(getTesterType() == Type.DISTRIBUTED_TESTER);
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
        
        source.publish("testSPLPublishNoFilterSFilteredSubscribe", false);
        
        SPLStream sub = SPLStreams.subscribe(t, "testSPLPublishNoFilterSFilteredSubscribe", source.getSchema());
        
        TStream<String> subscribe = sub.transform(new GetTupleId());

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
                
        source = addStartupDelay(source);
        
        source.publish("testSPLPublishAllowFilterWithSubscribe", true);
        
        SPLStream sub = SPLStreams.subscribe(t, "testSPLPublishAllowFilterWithSubscribe", source.getSchema());
        
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
              "testSPLPublishAllowFilterWithFilteredSubscribe", true);
  }
    @Test
    public void testSPLPublishNoFilterWithFilteredSubscribe() throws Exception {
        _testSPLPublishFilteredSubscribe(
              "testSPLPublishNoFilterWithFilteredSubscribe", false);
  }
  private void _testSPLPublishFilteredSubscribe(String topic, boolean allowFilters) throws Exception {
        final Topology t = new Topology();
        
        SPL.addToolkit(t, new File(getTestRoot(), "spl/testtk"));
        
        SPLStream source = SPLStreamsTest.testTupleStream(t);
                
        source = addStartupDelay(source);
        
        source.publish(topic, allowFilters);
        
        // Filter on the vi attribute, passing the value 321.
        Map<String,Object> params = new HashMap<>();
        params.put("topic", topic);
        params.put("value", 43675232L);        
  
        SPLStream sub = SPL.invokeSource(t, "testspl::Int64SubscribeFilter", params, source.getSchema());
        
        TStream<String> subscribe = sub.transform(new GetTupleId());

        completeAndValidate(subscribe, 20, "SPL:1");
    } 
}
