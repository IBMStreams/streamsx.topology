/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URL;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.ProcessingElement;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.StreamsConnection;

/**
 * Tests that only make sense with an on-prem streams.
 *
 */
public class StreamsOnlyConnectionTest {

    @Test
    public void testBadConnections() throws Exception {

        URL correctUrl = new URL(System.getenv("STREAMS_REST_URL"));
        
        URL badUrl = new URL(correctUrl.getProtocol(), correctUrl.getHost(),
        		     correctUrl.getPort(), "/streams/re");      
        System.err.println("BAD1:" + badUrl.toExternalForm());
        

        // send in wrong url
        StreamsConnection badConn = StreamsConnection.createInstance(null, null, badUrl.toExternalForm());
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 404, r.getStatusCode());
        }

        // send in url too long
        badUrl = new URL(correctUrl.getProtocol(), correctUrl.getHost(),
   		     correctUrl.getPort(), "/streams/rest/resourcesTooLong");      
        System.err.println("BAD2:" + badUrl.toExternalForm());

        badConn = StreamsConnection.createInstance(null, null, badUrl.toExternalForm());
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 404, r.getStatusCode());
        }

        // send in bad iName
        badConn = StreamsConnection.createInstance("fakeName", null, correctUrl.toExternalForm());
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 401, r.getStatusCode());
        }

        // send in wrong password
        badConn = StreamsConnection.createInstance(null, "badPassword", correctUrl.toExternalForm());
        badConn.allowInsecureHosts(true);
        try {
            badConn.getInstances();
        } catch (RESTException r) {
            assertEquals(r.toString(), 401, r.getStatusCode());
        }
    }

    @Test
    public void testGetInstances() throws Exception {
        StreamsConnection connection = StreamsConnection.createInstance(null, null, null);
        connection.allowInsecureHosts(true);
        // get all instances in the domain
        List<Instance> instances = connection.getInstances();
        // there should be at least one instance
        assertTrue(instances.size() > 0);
        
        Instance i2;
        String instanceName = System.getenv("STREAMS_INSTANCE_ID");
        if (instanceName != null) {

            i2 = connection.getInstance(instanceName);
            assertEquals(instanceName, i2.getId());
            
            i2.refresh();
            assertEquals(instanceName, i2.getId());
        } else {
        	i2 = instances.get(0);
        }
        
        List<ProcessingElement> instancePes = i2.getPes();
        for (ProcessingElement pe : instancePes) {
            assertNotNull(pe);
        }
        
        for (Instance instance : instances)
            StreamsConnectionTest.checkDomainFromInstance(instance);

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
