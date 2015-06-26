/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package topic;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

/**
 * Application which subscribes to a topic.
 * 
 * @see PublishBeacon Sample application that
 * publishes a topic
 * @see <a href="../../../spldoc/html/tk$com.ibm.streamsx.topology/ns$com.ibm.streamsx.topology.topic$1.html">Integration with SPL applications</a>
 */
public class SubscribeBeacon {

    /**
     * Submit this application which subscribes to a topic.
     * @param args Command line arguments, accepts a single optional topic name.
     * @throws Exception Error running the application
     */
    public static void main(String[] args) throws Exception {
        String topic = "/beacon";
        String type = "DISTRIBUTED";

        if (args.length >= 1)
            topic = "/" + args[0];
        if (args.length == 2)
            type = args[1];
            

        Topology topology = new Topology("SubscribeBeacon");

        TStream<BeaconTuple> beacon = topology.subscribe(topic,
                BeaconTuple.class);
        beacon.print();

        StreamsContextFactory.getStreamsContext(type)
                .submit(topology);
    }
}
