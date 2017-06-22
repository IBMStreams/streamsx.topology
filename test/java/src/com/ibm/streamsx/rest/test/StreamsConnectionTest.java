/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

public class StreamsConnectionTest {

    StreamsConnection connection;
    String instanceName;
    Instance instance;
    Job job;
    String jobId;
    String testType;

    public StreamsConnectionTest() {
    }

    public void setupConnection() throws Exception {
        if (connection == null) {
            String userName = System.getenv("USER");
            instanceName = System.getenv("STREAMS_INSTANCE_ID");
            String streamsPort = System.getenv("streamsPort");
            String instancePassword = System.getenv("STREAMS_INSTANCE_PASSWORD");

            testType = "DISTRIBUTED";

            // Default to QSE
            if ("streamsadmin".equals(userName) && instancePassword == null) {
                instancePassword = "passw0rd";
            }
            
            System.out.println("UserName: " + userName);

            if ((streamsPort == null) || streamsPort.isEmpty()) {
                // if port not specified, assume default one
                streamsPort = "8443";
            }
            System.out.println("InstanceName: " + instanceName);
            System.out.println("InstancePWD: " + instancePassword);

            // if the instance name and password are not set, bail
            assumeNotNull(instanceName, instancePassword);

            String restUrl = "https://localhost:" + streamsPort + "/streams/rest";
            connection = StreamsConnection.createInstance(userName, instancePassword, restUrl);

            // for localhost, need to disable security
            connection.allowInsecureHosts(true);
        }
    }

    public void setupInstance() throws Exception {
        setupConnection();

        if (instance == null) {
            instance = connection.getInstance(instanceName);
            // don't continue if the instance isn't started
            System.out.println("Instance: " + instance.getStatus());
            assumeTrue(instance.getStatus().equals("running"));
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

        try {
            // try a fake instance name
            connection.getInstance("fakeName");
            fail("the connection.getInstance call should have thrown an exception");
        } catch (RESTException r) {
            // not a failure, this is the expected result
            assertEquals(404, r.getStatusCode());
        }
    }

    @Before
    public void setupJob() throws Exception {
        setupInstance();
        if (jobId == null) {
            Topology topology = new Topology("JobForRESTApiTest");

            TStream<Integer> source = topology.periodicSource(randomGenerator(), 200, TimeUnit.MILLISECONDS);
            TStream<Integer> sourceDouble = source.transform(doubleNumber());
            @SuppressWarnings("unused")
            TStream<Integer> sourceDoubleAgain = sourceDouble.isolate().transform(doubleNumber());

            if (testType.equals("DISTRIBUTED")) {
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
        }
        System.out.println("jobId: " + jobId + " is setup.");
    }

    @SuppressWarnings("serial")
    static Function<Integer, Integer> doubleNumber() {
        return new Function<Integer, Integer>() {
            @Override
            public Integer apply(Integer v) {
                return new Integer(v * 2);
            }
        };
    }

    @SuppressWarnings("serial")
    static Supplier<Integer> randomGenerator() {
        return new Supplier<Integer>() {

            @Override
            public Integer get() {
                return (int) (Math.random() * 5000 + 1);
            }
        };
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
        Job job2 = instance.getJob(jobId);

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
    }

    @Test
    public void testCancelSpecificJob() throws Exception {
        if (jobId != null) {
            // cancel the job
            boolean cancel = connection.cancelJob(jobId);
            assertTrue(cancel == true);
            // remove these so @After doesn't fail
            job = null;
            jobId = null;
        }
    }

    @Test
    public void testNonExistantJob() throws Exception {
        try {
            // get a non-existant job
            @SuppressWarnings("unused")
            Job nonExistantJob = instance.getJob("999999");
            fail("this job number should not exist");
        } catch (RESTException r) {
            assertEquals(404, r.getStatusCode());
            assertEquals("CDISW5000E", r.getStreamsErrorMessageId());
        }

        // cancel a non-existant jobid
        boolean failCancel = connection.cancelJob("99999");
        assertTrue(failCancel == false);

    }

    @Test
    public void testOperators() throws Exception {

        List<Operator> operators = job.getOperators();

        // there should be 3 operators for this test, ordered by name
        assertEquals(3, operators.size());
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
        assertEquals("IntegerPeriodicMultiSource_OUT0", opSource.getName());

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
        assertEquals("com.ibm.streamsx.topology.functional.java::FunctionTransform", op1.getOperatorKind());

        List<InputPort> inputTransform = op1.getInputPorts();
        assertEquals(1, inputTransform.size());
        InputPort ip = inputTransform.get(0);
        assertEquals("IntegerTransformInteger_IN0", ip.getName());
        assertEquals(0, ip.getIndexWithinOperator());
        assertEquals("operatorInputPort", ip.getResourceType(), "operatorInputPort");

        List<Metric> inputPortMetrics = ip.getMetrics();
        for (Metric m : inputPortMetrics) {
            assertTrue((m.getMetricKind().equals("counter")) || (m.getMetricKind().equals("gauge")));
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
        assertEquals("IntegerTransformInteger_OUT0", opTransform.getName());
        assertEquals("IntegerTransformInteger_OUT0", opTransform.getStreamName());

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

    @Test
    public void testProcessingElements() throws Exception {

        List<ProcessingElement> pes = job.getPes();

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
    }

}
