// Tests for Instance, particularly ofEndpoint()

package com.ibm.streamsx.rest.test;

import com.ibm.streamsx.rest.Instance;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class InstanceTest {

    @BeforeClass
    public static void maybeSkip() {
        assumeTrue(null == System.getenv("STREAMS_DOMAIN_ID"));
    }

    @Test
    public void testOfEndpoint_configured() throws IOException {
        assumeNotNull(System.getenv("CP4D_URL"));
        assumeNotNull(System.getenv("STREAMS_INSTANCE_ID"));

        Instance instance = Instance.ofEndpoint(null, null, null, null, sslVerify());
        check(instance);
    }

    @Test
    public void testOfEndpoint_standalone() throws IOException {
        assumeTrue(null == System.getenv("CP4D_URL"));
        assumeNotNull(System.getenv("STREAMS_REST_URL"));

        Instance instance = Instance.ofEndpoint(null, null, null, null, sslVerify());
        check(instance);
    }

    private static void check(Instance instance) {
        assertNotNull(instance);
        assertNotNull(instance.getId());
        System.err.println(instance.getId());
        assertNotNull(instance.getHealth());
        System.err.println(instance.getHealth());
        assertNotNull(instance.getStatus());
        System.err.println(instance.getStatus());
    }

    private static boolean sslVerify() {
        String v = System.getProperty("topology.test.SSLVerify");
        if (v == null)
            return true;

        return Boolean.valueOf(v);
    }
}
