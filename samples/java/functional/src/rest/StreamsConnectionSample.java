/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package rest;

import java.io.IOException;
import java.util.List;

import com.ibm.streamsx.rest.InputPort;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.rest.Operator;
import com.ibm.streamsx.rest.OutputPort;
import com.ibm.streamsx.rest.ProcessingElement;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.StreamsConnection;

/**
 * This is the main class to show how to use the StreamsConnectionInterface.
 *
 * <p>
 * The example connects to the streams instance and gets a list of resources The
 * objects printed out include all the fields known in the JSON object per the
 * REST api however, the classes may not have accessors for all objects
 * </p>
 *
 * <p>
 * The following arguments are required for the sample:
 * <ul>
 * <li>userName</li>
 * <li>password</li>
 * <li>rest url for the IBM Streams Instance (streamstool geturl --api)</li>
 * <li>instance name</li>
 * </ul>
 * </p>
 */
public class StreamsConnectionSample {

    public static void main(String[] args) throws IOException {
        String userName = args[0];
        String authToken = args[1];
        String url = args[2];
        String instanceName = args[3];

        /*
         * Create the connection to the instance indicated
         */
        StreamsConnection sClient = StreamsConnection.createInstance(userName, authToken,
                url);
        /*
         * This option is only used to by-pass the certificate certification
         */
        if (args.length == 5 && "true".equals(args[4])) {
            sClient.allowInsecureHosts(true);
        }

        try {
            System.out.println("Instance: ");
            Instance instance = sClient.getInstance(instanceName);

            /*
             * From the Instance, get a list of jobs
             */
            List<Job> jobs = instance.getJobs();
            for (Job job : jobs) {
                System.out.println("Job: " + job.toString());
                /*
                 * For each job, get a list of operators
                 */
                List<Operator> operators = job.getOperators();
                for (Operator op : operators) {
                    System.out.println("Operator: " + op.toString());
                    List<Metric> metrics = op.getMetrics();
                    /*
                     * For each operator, you can get a list of metrics, output
                     * ports and input ports
                     */
                    for (Metric m : metrics) {
                        System.out.println("Metric: " + m.toString());
                    }
                    List<OutputPort> outP = op.getOutputPorts();
                    for (OutputPort oport : outP) {
                        System.out.println("Output Port: " + oport.toString());
                        /*
                         * For each output port, you can get a list of metrics
                         */
                        for (Metric om : oport.getMetrics()) {
                            System.out.println("Output Port Metric: " + om.toString());
                        }
                    }
                    List<InputPort> inP = op.getInputPorts();
                    /*
                     * For each input port, get a list of metrics
                     */
                    for (InputPort ip : inP) {
                        System.out.println("Input Port: " + ip.toString());
                        for (Metric im : ip.getMetrics()) {
                            System.out.println("Input Port Metric: " + im.toString());
                        }
                    }
                }
                /*
                 * For each job, get a list of processing elements
                 */
                for (ProcessingElement pe : job.getPes()) {
                    System.out.println("ProcessingElement:" + pe.toString());
                }
            }

            try {
                /*
                 * Get a specific job in the instance
                 */
                instance.getJob("99999");
            } catch (RESTException e) {
                /*
                 * This shows what is available in the RESTException should
                 * something fail
                 */
                System.out.println("Status Code: " + e.getStatusCode());
                System.out.println("Message Id: " + e.getStreamsErrorMessageId());
                System.out.println("MessageAsJson: " + e.getStreamsErrorMessageAsJson().toString());
                System.out.println("Message: " + e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
