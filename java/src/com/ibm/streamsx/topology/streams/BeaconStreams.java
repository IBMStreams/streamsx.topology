/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.streams;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.logic.Identity;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

/**
 * Utilities for beacon streams.
 * 
 */
public class BeaconStreams {

    public static class BeaconFunction implements Function<Long, BeaconTuple> {
        private static final long serialVersionUID = 1L;

        @Override
        public BeaconTuple apply(Long v) {
            return new BeaconTuple(v);
        }

    }

    /**
     * Creates an infinite source stream of tuples with no delay between each tuple.
     * The first tuple has the {@link BeaconTuple#getSequence() sequence} of zero, each subsequent tuple has
     * a sequence one higher than the previous tuple. The {@link BeaconTuple#getTime() time} of each tuple
     * is the time the tuple (object) was created.
     * 
     * @param te Topology element representing the topology the source stream will be contained in.
     * 
     * @return A stream that will contain {@link BeaconTuple} instances.
     */
    public static TStream<BeaconTuple> beacon(TopologyElement te) {
        return te.topology().endlessSourceN(new BeaconFunction());
    }

    /**
     * Creates a source stream of {@code count} tuples with no delay between each
     * tuple.
     * 
     * @param te Topology element representing the topology the source stream will be contained in.
     * 
     * @return A stream that will contain {@code count} {@link BeaconTuple}
     *         instances.
     */
    public static TStream<BeaconTuple> beacon(TopologyElement te, long count) {
        return te.topology().limitedSourceN(new BeaconFunction(), count);
    }

    /**
     * Creates an infinite source stream of {@code count} tuples with no delay between each
     * tuple. The first tuple has the value zero, each subsequent tuple has
     * a value one higher than the previous tuple.
     * 
     * @param te Topology element representing the topology the source stream will be contained in.
     * @return A stream that will contain tuples of monotonically increasing {@code Long} tuples.
     */
    public static TStream<Long> longBeacon(TopologyElement te) {
        return te.topology().endlessSourceN(new Identity<Long>()).asType(Long.class);
    }

    /**
     * Creates a source stream of {@code count} tuples with no delay between each
     * tuple. The first tuple has the value zero, each subsequent tuple has
     * a value one higher than the previous tuple.
     * 
     * @param te Topology element representing the topology the source stream will be contained in.
     * @param count Number of tuples on the stream.
     * @return A stream that will contain {@code count} tuples.
     */
    public static TStream<Long> longBeacon(TopologyElement te, long count) {
        return te.topology().limitedSourceN(new Identity<Long>(), count).asType(Long.class);
    }
    
    /**
     * Produce a source stream declaring a single tuple, with the value 0.
     * @param te Topology element representing the topology the source stream will be contained in.
     * @return A stream that will contain a single tuple.
     */
    public static TStream<Long> single(TopologyElement te) {
        return longBeacon(te, 1);
    }
}
