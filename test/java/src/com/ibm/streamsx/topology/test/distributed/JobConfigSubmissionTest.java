/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2018
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.spl.JavaPrimitive;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.test.api.JobPropertiesTest.JobPropertiesTestOp;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Test submission using JobConfig API
 */
public class JobConfigSubmissionTest extends TestTopology {

    @Test
    public void testGroupJobConfig() throws Exception {
        
        JobConfig config = new JobConfig();
        config.setJobGroup("default");
        testItDirect("testGroupJobConfig", config, "<jobId>", "<jobName>", "default", "<empty>");
    }
    @Test
    public void testNameJobConfig() throws Exception {
        
        JobConfig config = new JobConfig();
        String jobName = "nameG" + System.currentTimeMillis();
        config.setJobName(jobName);
        testItDirect("testNameJobConfig", config, "<jobId>", jobName, "default", "<empty>");
    }
    
    @Test
    public void testSubmitUsingInstance() throws Exception {
    	
    	assumeTrue(getTesterContext().getType() == Type.DISTRIBUTED_TESTER);
    	assumeTrue(System.getenv("CP4D_URL") != null);
        
        JobConfig config = new JobConfig();
        String jobName = "nameSC" + System.currentTimeMillis();
        config.setJobName(jobName);
        
        boolean verify = true;
        if (getConfig().containsKey(ContextProperties.SSL_VERIFY))
            verify = (Boolean) getConfig().get(ContextProperties.SSL_VERIFY);
        
        Instance instance = Instance.ofEndpoint((String) null, (String) null,
                (String) null, (String) null, verify);
        
        assertEquals(verify, instance.getStreamsConnection().isVerify());
        
        getConfig().put(ContextProperties.STREAMS_INSTANCE, instance);
        getConfig().remove(ContextProperties.SSL_VERIFY);

        final String cpd_url = setEnv("CP4D_URL", null);
        final String sid = setEnv("STREAMS_INSTANCE_ID", null);
        
        try {
            testItDirect("testSubmitUsingInstance", config, "<jobId>", jobName, "default", "<empty>");
        } finally {
            setEnv("CP4D_URL", cpd_url);
            setEnv("STREAMS_INSTANCE_ID", sid);
        }
    }
    @Test
    public void testSubmitUsingInstanceWithRemoteBuild() throws Exception {
    	
    	assumeTrue(getTesterContext().getType() == Type.DISTRIBUTED_TESTER);
    	assumeTrue(System.getenv("CP4D_URL") != null);
        
        JobConfig config = new JobConfig();
        String jobName = "nameSC" + System.currentTimeMillis();
        config.setJobName(jobName);
        
        boolean verify = true;
        if (getConfig().containsKey(ContextProperties.SSL_VERIFY))
            verify = (Boolean) getConfig().get(ContextProperties.SSL_VERIFY);
        
        Instance instance = Instance.ofEndpoint((String) null, (String) null,
                (String) null, (String) null, verify);
        getConfig().put(ContextProperties.STREAMS_INSTANCE, instance);
        getConfig().put(ContextProperties.FORCE_REMOTE_BUILD, true);
        getConfig().remove(ContextProperties.SSL_VERIFY);

        final String cpd_url = setEnv("CP4D_URL", null);
        final String sid = setEnv("STREAMS_INSTANCE_ID", null);
        
        assertNull(System.getenv("CP4D_URL"));
        assertNull(System.getenv("STREAMS_INSTANCE_ID"));
        
        try {
            testItDirect("testSubmitUsingInstanceWithRemoteBuild", config, "<jobId>", jobName, "default", "<empty>");
        } finally {
            setEnv("CP4D_URL", cpd_url);
            setEnv("STREAMS_INSTANCE_ID", sid);
        }
    }
    
    @SuppressWarnings("unchecked")
    private static String setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            if (value == null) {
                return writableEnv.remove(key);
            } else {
                writableEnv.put(key, value);
                return value;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to clear environment variable", e);
        }
    }
    
    @Test
    public void testDataDirJobConfig() throws Exception {
        
        JobConfig config = new JobConfig();
        String jobName = "nameDD" + System.currentTimeMillis();
        config.setJobName(jobName);
        config.setDataDirectory("/tmp/some/dir");
        testItDirect("testDataDirJobConfig", config, "<jobId>", jobName, "default", "/tmp/some/dir");
    }
    
    private void testItDirect(String topologyName, JobConfig config, String ...expected)
            throws Exception {
        
        // Uses a Java primitive operator directly.
        assumeTrue(hasStreamsInstall());

        // JobConfig only apply to DISTRIBUTED submit
        assumeTrue(isDistributedOrService());
        
        config.addToConfig(getConfig());

        Topology topology = newTopology(topologyName);
        topology.addClassDependency(JobPropertiesTestOp.class);
        SPLStream sourceSPL = JavaPrimitive.invokeJavaPrimitiveSource(topology, JobPropertiesTestOp.class,
                SPLSchemas.STRING, null);
        TStream<String> source = sourceSPL.toStringStream();

        Condition<Long> end = topology.getTester().tupleCount(source, 4);
        Condition<List<String>> result = topology.getTester().stringContents(source, expected);
        complete(topology.getTester(), end.and(result), 10, TimeUnit.SECONDS);
        
        
        assertTrue(result.valid());
        assertTrue(end.valid());
    }
    
}
