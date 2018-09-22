/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
