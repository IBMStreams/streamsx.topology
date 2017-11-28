/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package rest;

import java.util.List;

import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.rest.InputPort;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.rest.Operator;
import com.ibm.streamsx.rest.OutputPort;
import com.ibm.streamsx.rest.StreamingAnalyticsService;

/**
 * Sample code to show how to access a Streaming Analytics Instance through the
 * REST Api
 * 
 * <p>
 * The example connects to the streams instance and gets a list of resources.
 * The objects printed out include all the fields known in the JSON object per
 * the REST api however, the classes may not have accessors for all objects.
 * </p>
 *
 * <p>
 * The following arguments are required for the sample:
 * <ul>
 * <li>credentials (as an absolute file path, or JSON string)</li>
 * <li>serviceName identifying the credentials to use in the file</li>
 * </ul>
 * </p>
 *
 */
public class StreamingAnalyticsConnectionSample {

    public static void main(String[] args) {
        String credentials = args[0];
        String serviceName = args[1];

        System.out.println(credentials);
        System.out.println(serviceName);

        try {
            StreamingAnalyticsService sClient = StreamingAnalyticsService.of(
                    new JsonPrimitive(credentials),
                    serviceName);

            Instance instance = sClient.getInstance();
            System.out.println("Instance:" + instance.toString());

            if (!instance.getStatus().equals("running")) {
                System.out.println("Instance is not started, please start the instance to retrieve more information");
                System.exit(0);
            }
            /* Retrieve a list of jobs in the instance */
            List<Job> jobs = instance.getJobs();
            for (Job job : jobs) {
                System.out.println("Job: " + job.toString());
                /* Retrieve a list of operators for the current job */
                List<Operator> operators = job.getOperators();
                for (Operator op : operators) {
                    System.out.println("Operator: " + op.toString());
                    /* Retrieve a list of metrics for the current operator */
                    List<Metric> metrics = op.getMetrics();
                    for (Metric m : metrics) {
                        System.out.println("Metric: " + m.toString());
                    }
                    /*
                     * Retrieve a list of output ports for the current operator
                     */
                    List<OutputPort> outP = op.getOutputPorts();
                    for (OutputPort oport : outP) {
                        System.out.println("Output Port: " + oport.toString());
                        /* Retrieve the metrics for this output port */
                        for (Metric om : oport.getMetrics()) {
                            System.out.println("Output Port Metric: " + om.toString());
                        }
                    }
                    /*
                     * Retrieve a list of input ports for the current operator
                     */
                    List<InputPort> inP = op.getInputPorts();
                    for (InputPort ip : inP) {
                        System.out.println("Input Port: " + ip.toString());
                        /* Retrieve the metrics for this input port */
                        for (Metric im : ip.getMetrics()) {
                            System.out.println("Input Port Metric: " + im.toString());
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
