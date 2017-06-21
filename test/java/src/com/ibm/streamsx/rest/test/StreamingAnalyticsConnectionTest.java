/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.StreamingAnalyticsConnection;

public class StreamingAnalyticsConnectionTest extends StreamsConnectionTest {

    @Override
    public void setupConnection() throws Exception {
        if (connection == null) {
            String serviceName = System.getenv("STREAMING_ANALYTICS_SERVICE_NAME");
            String vcapServices = System.getenv("VCAP_SERVICES");

            // if we don't have serviceName or vcapServices, skip the test
            assumeNotNull(serviceName, vcapServices);

            testType = "STREAMING_ANALYTICS_SERVICE";
            connection = StreamingAnalyticsConnection.createInstance(vcapServices, serviceName);
        }
    }

    @Override
    public void setupInstance() throws Exception {
        setupConnection();
        if (instance == null) {
            instance = ((StreamingAnalyticsConnection) connection).getInstance();
            // bail if streaming analytics instance isn't up & running
            assumeTrue(instance.getStatus().equals("running"));
        }
    }

    @Override
    @Test
    public void testGetInstances() throws Exception {
        setupConnection();

        // get all instances in the domain
        List<Instance> instances = connection.getInstances();
        // there should be at least one instance
        assertEquals(1, instances.size());

        String instanceName = instances.get(0).getId();
        Instance i2 = connection.getInstance(instanceName);
        assertEquals(instanceName, i2.getId());

        try {
            // try a fake instance name
            connection.getInstance("fakeName");
            fail("the connection.getInstance() call should have thrown an exception");
        } catch (RESTException r) {
            assertEquals(404, r.getStatusCode());
        }
    }
}
