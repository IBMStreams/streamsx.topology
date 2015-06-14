/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package topic;

import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.streams.BeaconStreams;

public class PublishBeacon {

    public static void main(String[] args) throws Exception {
        String topic = "/beacon";

        if (args.length == 1)
            topic = "/" + args[0];

        Topology topology = new Topology("PublishBeacon");

        BeaconStreams.beacon(topology).throttle(500, TimeUnit.MILLISECONDS)
                .publish(topic);

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.BUNDLE)
                .submit(topology);
    }
}
