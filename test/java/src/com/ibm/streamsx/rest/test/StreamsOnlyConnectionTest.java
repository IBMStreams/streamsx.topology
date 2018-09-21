/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.rest.Domain;
import com.ibm.streamsx.rest.InputPort;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.rest.Operator;
import com.ibm.streamsx.rest.OutputPort;
import com.ibm.streamsx.rest.PEInputPort;
import com.ibm.streamsx.rest.PEOutputPort;
import com.ibm.streamsx.rest.ProcessingElement;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.Resource;
import com.ibm.streamsx.rest.ResourceAllocation;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;

/**
 * Tests that only make sense with an on-prem streams.
 *
 */
public class StreamsOnlyConnectionTest extends StreamsConnectionTest{

    @Test
    public void testBadConnections() throws Exception {

        String sPort = getStreamsPort();

        // send in wrong url
        String badUrl = "https://localhost:" + sPort + "/streams/re";
        StreamsConnection badConn = StreamsConnection.createInstance(null, null, badUrl);
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 404, r.getStatusCode());
        }

        // send in url too long
        String badURL = "https://localhost:" + sPort + "/streams/rest/resourcesTooLong";
        badConn = StreamsConnection.createInstance(null, null, badURL);
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 404, r.getStatusCode());
        }

        // send in bad iName
        String restUrl = "https://localhost:" + sPort + "/streams/rest/resources";
        badConn = StreamsConnection.createInstance("fakeName", null, restUrl);
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 401, r.getStatusCode());
        }

        // send in wrong password
        badConn = StreamsConnection.createInstance(null, "badPassword", restUrl);
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 401, r.getStatusCode());
        }
    }

    @Test
    public void testGetInstances() throws Exception {
        setupConnection();
        // get all instances in the domain
        List<Instance> instances = connection.getInstances();
        // there should be at least one instance
        assertTrue(instances.size() > 0);

        Instance i2 = connection.getInstance(instanceName);
        assertEquals(instanceName, i2.getId());

        i2.refresh();
        assertEquals(instanceName, i2.getId());
        
        List<ProcessingElement> instancePes = i2.getPes();
        for (ProcessingElement pe : instancePes) {
            assertNotNull(pe);
        }
        
        for (Instance instance : instances)
            checkDomainFromInstance(instance);

        try {
            // try a fake instance name
            connection.getInstance("fakeName");
            fail("the connection.getInstance call should have thrown an exception");
        } catch (RESTException r) {
            // not a failure, this is the expected result
            assertEquals(r.toString(), 404, r.getStatusCode());
        }
    }

}
