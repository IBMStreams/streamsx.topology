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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
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
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;

public class StreamsConnectionTest {

    protected StreamsConnection connection;
    String instanceName;
    Instance instance;
    Job job;
    String jobId;
    String testType;

    public StreamsConnectionTest() {
    }
    
    public static boolean sslVerify() {
        String v = System.getProperty("topology.test.SSLVerify");
        if (v == null)
            return true;
        
        return Boolean.valueOf(v);
    }

    protected void setupConnection() throws Exception {
        if (connection == null) {
            testType = "DISTRIBUTED";

            instanceName = System.getenv("STREAMS_INSTANCE_ID");
            if (instanceName != null)
                System.out.println("InstanceName: " + instanceName);
            else
            	System.out.println("InstanceName: assumng single instance");
            	

            connection = StreamsConnection.createInstance(null, null, null);

            if (!sslVerify())
                connection.allowInsecureHosts(true);
        }
    }

    protected void setupInstance() throws Exception {
        setupConnection();

        if (instance == null) {
			if (instanceName != null) {
				instance = connection.getInstance(instanceName);
			} else {
				List<Instance> instances = connection.getInstances();
				assertEquals(1, instances.size());
				instance = instances.get(0);
			}
            // don't continue if the instance isn't started
            assumeTrue(instance.getStatus().equals("running"));
        }
    }


    static void checkDomainFromInstance(Instance instance)  throws Exception {
        instance.refresh();
        
        Domain domain = instance.getDomain();
		if (domain != null) {
			assertNotNull(domain.getId());
			assertNotNull(domain.getZooKeeperConnectionString());
			assertNotNull(domain.getCreationUser());
			assertTrue(domain.getCreationTime() <= instance.getCreationTime());
		}
        
        checkResourceAllocations(instance.getResourceAllocations(), false);
    }

    public void setupJob() throws Exception {
        setupInstance();
		// avoid clashes with sub-class tests
		Topology topology = new Topology(getClass().getSimpleName(), "JobForRESTApiTest");

		TStream<Integer> source = topology.periodicSource(() -> (int) (Math.random() * 5000 + 1), 200,
				TimeUnit.MILLISECONDS);
		source.invocationName("IntegerPeriodicMultiSource");
		TStream<Integer> sourceDouble = source.map(doubleNumber());
		sourceDouble.invocationName("IntegerTransformInteger");
		sourceDouble.colocate(source);
		TStream<Integer> sourceDoubleAgain = sourceDouble.isolate().map(doubleNumber());
		sourceDoubleAgain.invocationName("ZIntegerTransformInteger");

		if (testType.equals("DISTRIBUTED")) {
		    Map<String,Object> cfg = new HashMap<>();
		    cfg.put(ContextProperties.SSL_VERIFY, sslVerify());
		    
			jobId = StreamsContextFactory.getStreamsContext(StreamsContext.Type.DISTRIBUTED).submit(topology).get()
					.toString();
		} else if (testType.equals("STREAMING_ANALYTICS_SERVICE")) {
			jobId = StreamsContextFactory.getStreamsContext(StreamsContext.Type.STREAMING_ANALYTICS_SERVICE)
					.submit(topology).get().toString();
		} else {
			fail("This test should be skipped");
		}

		job = instance.getJob(jobId);
		job.waitForHealthy(60, TimeUnit.SECONDS);

		assertEquals("healthy", job.getHealth());
        System.out.println("jobId: " + jobId + " is setup.");
    }

    static Function<Integer, Integer> doubleNumber() {
        return x -> x*2;
    }

    @After
    public void removeJob() throws Exception {
        if (job != null) {
            job.cancel();
            job = null;
        }
    }

    @Test
    public void testJobObject() throws Exception {
    	setupJob();
    	
        List<Job> jobs = instance.getJobs();
        // we should have at least one job
        assertTrue(jobs.size() > 0);
        boolean foundJob = false;
        for (Job j : jobs) {
            if (j.getId().equals(job.getId())) {
                foundJob = true;
                break;
            }
        }
        assertTrue(foundJob);

        // get a specific job
        final Job job2 = instance.getJob(jobId);

        for (int i = 0; i < 3; i++) {

            // check a subset of info returned matches
            assertEquals(job.getId(), job2.getId());
            assertEquals(job.getName(), job2.getName());
            assertEquals(job.getHealth(), job2.getHealth());
            assertEquals(job.getApplicationName(), job2.getApplicationName());
            assertEquals(job.getJobGroup(), job2.getJobGroup());
            assertEquals(job.getStartedBy(), job2.getStartedBy());
            assertEquals(job.getStatus(), job2.getStatus());
            assertEquals("job", job2.getResourceType());
            assertEquals("job", job.getResourceType());

            Thread.sleep(400);
            job2.refresh();
        }

        // job is setup with 3 operators
        List<Operator> operators = job.getOperators();
        assertEquals(3, operators.size());

        // job is setup with 2 PEs
        List<ProcessingElement> pes = job.getPes();
        assertEquals(2, pes.size());
        
        checkResourceAllocations(job.getResourceAllocations(), true);
        
        validateOperators();
        validateProcessingElements();
    }

    @Test
    public void testCancelSpecificJob() throws Exception {
    	setupJob();
		// cancel the job
		boolean cancel = job.cancel();
		assertTrue(cancel == true);
		// remove these so @After doesn't fail
		job = null;
		jobId = null;
    }

    @Test
    public void testNonExistantJob() throws Exception {
    	setupInstance();
        try {
            // get a non-existant job
            instance.getJob("9999999999");
            fail("this job number should not exist");
        } catch (RESTException r) {
            assertEquals(r.toString(), 404, r.getStatusCode());
            assertEquals("CDISW5000E", r.getStreamsErrorMessageId());
        }
    }

    private void validateOperators() throws Exception {
        List<Operator> operators = job.getOperators();

        // there should be 3 operators for this test, ordered by name
        assertEquals(3, operators.size());
        
       List<ProcessingElement> jobpes = job.getPes();
        for (Operator op : operators) {
            ProcessingElement pe = op.getPE();
            assertNotNull(pe);
            boolean inJobList = false;
            for (ProcessingElement pej : jobpes) {
                if (pej.getId().equals(pe.getId())) {
                    inJobList = true;
                    break;
                }
            }
            assertTrue("PE not in job list:" + pe.getId(), inJobList);
        }     
        
        // the first operator will have an output port
        Operator op0 = operators.get(0);
        assertEquals("operator", op0.getResourceType());
        assertEquals("IntegerPeriodicMultiSource", op0.getName());
        assertEquals(0, op0.getIndexWithinJob());
        assertEquals("com.ibm.streamsx.topology.functional.java::FunctionPeriodicSource", op0.getOperatorKind());

        List<InputPort> inputSource = op0.getInputPorts();
        assertEquals(0, inputSource.size());

        List<OutputPort> outputSource = op0.getOutputPorts();
        assertEquals(1, outputSource.size());
        OutputPort opSource = outputSource.get(0);
        assertEquals(0, opSource.getIndexWithinOperator());
        assertEquals("operatorOutputPort", opSource.getResourceType());
        assertNameValid(opSource.getName());

        List<Metric> operatorMetrics = opSource.getMetrics();
        for (Metric m : operatorMetrics) {
            assertEquals(m.getMetricKind(), "counter");
            assertEquals(m.getMetricType(), "system");
            assertEquals(m.getResourceType(), "metric");
            assertNotNull(m.getName());
            assertNotNull(m.getDescription());
            assertTrue(m.getLastTimeRetrieved() > 0);
        }
        // this operator will have an input and an output port
        Operator op1 = operators.get(1);
        assertEquals("operator", op1.getResourceType());
        assertEquals("IntegerTransformInteger", op1.getName());
        assertEquals(1, op1.getIndexWithinJob());
        assertEquals("com.ibm.streamsx.topology.functional.java::Map", op1.getOperatorKind());

        List<InputPort> inputTransform = op1.getInputPorts();
        assertEquals(1, inputTransform.size());
        InputPort ip = inputTransform.get(0);
        assertNameValid(ip.getName());
        assertEquals(0, ip.getIndexWithinOperator());
        assertEquals("operatorInputPort", ip.getResourceType(), "operatorInputPort");

        List<Metric> inputPortMetrics = ip.getMetrics();
        for (Metric m : inputPortMetrics) {
            assertTrue("Unexpected metric kind for metric " + m.getName() + ": "
                    + m.getMetricKind(),
                    (m.getMetricKind().equals("counter")) ||
                            (m.getMetricKind().equals("gauge")) ||
                            (m.getMetricKind().equals("time")));
            assertEquals("system", m.getMetricType());
            assertEquals("metric", m.getResourceType());
            assertNotNull(m.getName());
            assertNotNull(m.getDescription());
            assertTrue(m.getLastTimeRetrieved() > 0);
        }

        List<OutputPort> outputTransform = op1.getOutputPorts();
        assertEquals(1, outputTransform.size());
        OutputPort opTransform = outputTransform.get(0);
        assertEquals(0, opTransform.getIndexWithinOperator());
        assertEquals("operatorOutputPort", opTransform.getResourceType());
        assertNameValid(opTransform.getName());
        assertNameValid(opTransform.getStreamName());

        List<Metric> outputPortMetrics = opTransform.getMetrics();
        for (Metric m : outputPortMetrics) {
            assertEquals("counter", m.getMetricKind());
            assertEquals("system", m.getMetricType());
            assertEquals("metric", m.getResourceType());
            assertNotNull(m.getName());
            assertNotNull(m.getDescription());
            assertTrue(m.getLastTimeRetrieved() > 0);
        }
    }
    
    static void assertNameValid(String name) {
        assertNotNull(name);
        assertFalse(name.isEmpty());
        
    }

    private void validateProcessingElements() throws Exception {
    	

        final List<ProcessingElement> pes = job.getPes();

        // there should be 2 processing element for this test
        assertEquals(2, pes.size());
        
        

        ProcessingElement pe1 = pes.get(0);
        assertEquals(0, pe1.getIndexWithinJob());
        assertTrue(pe1.getStatus().equals("running") || pe1.getStatus().equals("starting"));
        assertEquals("none", pe1.getStatusReason());
        assertTrue(pe1.getProcessId() != null);
        assertEquals("pe", pe1.getResourceType());

        // PE metrics
        List<Metric> peMetrics = pe1.getMetrics();
        for (int i = 0; i < 10; i++) {
            if (peMetrics.size() > 0) {
                break;
            }
            Thread.sleep(50);
            peMetrics = pe1.getMetrics();
        }
        assertTrue(peMetrics.size() > 0);
        for (Metric m : peMetrics) {
            assertTrue((m.getMetricKind().equals("counter")) || (m.getMetricKind().equals("gauge")));
            assertEquals("system", m.getMetricType());
            assertEquals("metric", m.getResourceType());
            assertNotNull(m.getName());
            assertNotNull(m.getDescription());
            assertTrue(m.getLastTimeRetrieved() > 0);
        }
        Metric m = peMetrics.get(0);
        long lastTime = m.getLastTimeRetrieved();
        Thread.sleep(3500);
        m.refresh();
        assertTrue(lastTime < m.getLastTimeRetrieved());

        String pid = pe1.getProcessId();
        pe1.refresh();
        assertEquals(pid, pe1.getProcessId());

        List<PEInputPort> inputPorts = pe1.getInputPorts();
        assertTrue(inputPorts.size() == 0);

        List<PEOutputPort> outputPorts = pe1.getOutputPorts();
        assertTrue(outputPorts.size() == 1);

        PEOutputPort op = outputPorts.get(0);
        assertEquals(0, op.getIndexWithinPE());
        assertEquals("peOutputPort", op.getResourceType());
        assertEquals("tcp", op.getTransportType());

        // PE Output Port metrics
        List<Metric> outputPortMetrics = op.getMetrics();
        assertTrue(outputPortMetrics.size() > 0);
        for (Metric opMetric : outputPortMetrics) {
            assertTrue((opMetric.getMetricKind().equals("counter")) || (opMetric.getMetricKind().equals("gauge")));
            assertEquals("system", opMetric.getMetricType());
            assertEquals("metric", opMetric.getResourceType());
            assertNotNull(opMetric.getName());
            assertNotNull(opMetric.getDescription());
            assertTrue(opMetric.getLastTimeRetrieved() > 0);
        }

        ProcessingElement pe2 = pes.get(1);
        assertEquals(1, pe2.getIndexWithinJob());
        assertEquals("running", pe2.getStatus());
        assertEquals("none", pe2.getStatusReason());
        assertTrue(pe2.getProcessId() != null);
        assertEquals("pe", pe2.getResourceType());

        List<PEOutputPort> PE2OutputPorts = pe2.getOutputPorts();
        assertTrue(PE2OutputPorts.size() == 0);

        List<PEInputPort> PE2inputPorts = pe2.getInputPorts();
        assertTrue(PE2inputPorts.size() == 1);

        // PE Input Port metrics
        PEInputPort ip = PE2inputPorts.get(0);
        List<Metric> inputPortMetrics = ip.getMetrics();
        assertTrue(inputPortMetrics.size() > 0);
        for (Metric ipMetric : inputPortMetrics) {
            assertTrue((ipMetric.getMetricKind().equals("counter")) || (ipMetric.getMetricKind().equals("gauge")));
            assertEquals("system", ipMetric.getMetricType());
            assertEquals("metric", ipMetric.getResourceType());
            assertNotNull(ipMetric.getName());
            assertNotNull(ipMetric.getDescription());
            assertTrue(ipMetric.getLastTimeRetrieved() > 0);
        }

        // operator for 2nd PE should point to the 3rd operator for job
        List<Operator> peOperators = pe2.getOperators();
        assertTrue(peOperators.size() == 1);
        List<Operator> jobOperators = job.getOperators();
        assertTrue(jobOperators.size() == 3);

        Operator peOp = peOperators.get(0);
        Operator jobOp = jobOperators.get(2);

        assertEquals(peOp.getName(), jobOp.getName());
        assertEquals(peOp.getIndexWithinJob(), jobOp.getIndexWithinJob());
        assertEquals(peOp.getResourceType(), jobOp.getResourceType());
        assertEquals(peOp.getOperatorKind(), jobOp.getOperatorKind());
        
        for (ProcessingElement pe : pes) {
            checkResourceAllocation(pe.getResourceAllocation(), true);
        }
    }
    
    private static void checkResourceAllocations(List<ResourceAllocation> ras, boolean app)
        throws IOException {
        for (ResourceAllocation ra : ras)
            checkResourceAllocation(ra, app);
    }

    private static void checkResourceAllocation(ResourceAllocation ra, boolean app) throws IOException {
    	if (ra == null)
    		return;
        assertEquals("resourceAllocation", ra.getResourceType());
        if (app)
            assertTrue(ra.isApplicationResource());
        assertNotNull(ra.getSchedulerStatus());
        assertNotNull(ra.getStatus());
        
        Instance rai = ra.getInstance();
        for (ProcessingElement pe : ra.getPes()) {
            assertNotNull(pe);
            assertNotNull(pe.getStatus());
        }
        for (Job job : ra.getJobs()) {
            assertNotNull(job);
            assertNotNull(job.getStatus());
        }
        assertSame(rai, ra.getInstance());
        
        Resource r = ra.getResource();
        assertNotNull(r.getId());
        assertNotNull(r.getDisplayName());
        assertNotNull(r.getIpAddress());       
        assertEquals("resource", r.getResourceType());
        
        
        
        for (Metric metric : r.getMetrics()) {
            assertTrue((metric.getMetricKind().equals("counter")) || (metric.getMetricKind().equals("gauge")));
            assertEquals("system", metric.getMetricType());
            assertEquals("metric", metric.getResourceType());
            assertNotNull(metric.getName());
            assertNotNull(metric.getDescription());
            assertTrue(metric.getLastTimeRetrieved() > 0);
        }
        

    }
}
