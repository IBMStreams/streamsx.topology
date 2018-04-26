package bluemix;

import java.io.File;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.AnalyticsServiceProperties;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

/**
 * Sample demonstrating submission of a topology
 * to Streaming Analytic service on IBM Cloud.. 
 *
 */
public class Submit2StreamingAnalyticService {
    
    public static void main(String[] args) throws Exception {
        
        String vcapFile = args[0];
        String serviceName = args[1];

        /*
         * Create a simple topology, focus of the
         * sample is the submission of the job
         * to the service.
         * 
         * This topology is just like simple.HelloWorld
         * with different values printed to the console log.
         */
        Topology topology = new Topology("Submit2StreamingAnalyticService");
        topology.strings(
                "Hello", "Streaming Analytic Service",
                serviceName, "running on IBM Cloud").print();       
        
        // Require a configuration object.
        Map<String,Object> config = new HashMap<>();
        
        // Here the VCAP_SERVICES information is in a local file
        // (as serialized JSON)
        config.put(AnalyticsServiceProperties.VCAP_SERVICES, new File(vcapFile));
        
        // Explicitly state which service is the job will be submitted to
        // The service must be in the Streaming Analytic Service section
        // of the VCAP services.
        config.put(AnalyticsServiceProperties.SERVICE_NAME, serviceName);
        
        // Optionally we can specify a job name
        // (note job names must be unique within the instance).
        JobConfig jco = new JobConfig();
        jco.setJobName("StreamingAnalyticsSubmitSample");
        
        config.put(JobProperties.CONFIG, jco);
        
        // Submit to the ANALYICS_SERVICE context
        @SuppressWarnings("unchecked")
        StreamsContext<BigInteger> context =
             (StreamsContext<BigInteger>) StreamsContextFactory.getStreamsContext(Type.STREAMING_ANALYTICS_SERVICE);
        
        BigInteger jobId = context.submit(topology, config).get();

        System.out.println("Submitted job with jobId=" + jobId);
        
    }
}
