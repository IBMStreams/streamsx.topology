/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package topic;

import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.streams.BeaconStreams;

/**
 * Publishes a beacon stream on a topic.
 * A published stream can be subscribed to by
 * other applications. This allows multiple
 * applications to consume tuples from an
 * application. For example an application
 * that ingests & prepares data can be consumed
 * by multiple analytic applications.
 * <BR>
 * Multiple applications (or multiple streams within applications)
 * can publish to the same topic.
 * <BR>
 * Applications dynamically subscribe
 * to a published topic, so that new applications
 * that subscribe to a topic can be submitted at
 * any time.
 * 
 * @see SubscribeBeacon Sample application that
 * subscribes to the topic published by this application
 * @see <a href="../../../spldoc/html/tk$com.ibm.streamsx.topology/ns$com.ibm.streamsx.topology.topic$1.html">Integration with SPL applications</a>
 */
public class PublishBeacon {

    /**
     * Submit this application which publishes a stream.
     * @param args Command line arguments, accepts a single optional topic name.
     * @throws Exception Error running the application
     */
    public static void main(String[] args) throws Exception {
        
        // Select the topic name for the command line
        // using '/beacon' if not supplied.
        String topic = "/beacon";
        String type = "DISTRIBUTED";

        if (args.length >= 1)
            topic = "/" + args[0];
        if (args.length == 2)
            type = args[1];

        Topology topology = new Topology("PublishBeacon");

        /*
         * Publish a throttled beacon stream to a topic
         */
        BeaconStreams.beacon(topology).throttle(100, TimeUnit.MILLISECONDS)
                .publish(topic);

        /*
         * Publish-Subscribe only works with distributed.
         */
        StreamsContextFactory.getStreamsContext(type)
                .submit(topology);
    }
}
