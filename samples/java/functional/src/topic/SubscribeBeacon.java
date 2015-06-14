/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package topic;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

public class SubscribeBeacon {

    public static void main(String[] args) throws Exception {
        String topic = "/beacon";

        if (args.length == 1)
            topic = "/" + args[0];

        Topology topology = new Topology("SubscribeBeacon");

        TStream<BeaconTuple> beacon = topology.subscribe(topic,
                BeaconTuple.class);
        beacon.print();

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.BUNDLE)
                .submit(topology);
    }
}
