/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.test.TestTopology.SC_OK;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.function7.Supplier;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

public class ParallelTest {
    @Test
    public void testParallelNonPartitioned() throws Exception {
        assumeTrue(SC_OK);

        Topology topology = new Topology("testParallel");
        final int count = new Random().nextInt(1000) + 37;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(5);

        @SuppressWarnings("serial")
        TStream<Integer> is = pb.transform(randomHashProducer(), Integer.class);
        TStream<Integer> joined = is.unparallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count), String.class);

        Tester tester = topology.getTester();
        Condition expectedCount = tester.tupleCount(numRegions, 1);
        Condition regionCount = tester.stringContents(numRegions, "5");

        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();

        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }
    
    @Test
    public void testParallelPartitioned() throws Exception {
        Topology topology = new Topology("testParallelPartition");
        final int count = new Random().nextInt(10) + 37;

        TStream<KeyableBeaconTuple> kb = topology.source(
                keyableBeacon5Counter(count), KeyableBeaconTuple.class);
        TStream<KeyableBeaconTuple> pb = kb.parallel(5);
        TStream<ChannelAndSequence> cs = pb.transform(channelSeqTransformer(),
                ChannelAndSequence.class);
        TStream<ChannelAndSequence> joined = cs.unparallel();

        TStream<String> valid_count = joined.transform(partitionCounter(count),
                String.class);

        Tester tester = topology.getTester();
        Condition expectedCount = tester.tupleCount(valid_count, 1);
        Condition validCount = tester.stringContents(valid_count, "5");

         StreamsContextFactory.getStreamsContext(StreamsContext.Type.STANDALONE_TESTER).submit(topology).get();

         assertTrue(expectedCount.valid());
         assertTrue(validCount.valid());
    }
    
    @Test
    public void testObjectHashPartition() throws Exception {
        Topology topology = new Topology("testObjectHashPartition");
        final int count = new Random().nextInt(10) + 37;

        TStream<BeaconTuple> kb = topology.source(
                beaconTuple5Counter(count), BeaconTuple.class);
        TStream<BeaconTuple> pb = kb.parallel(5, TStream.Routing.PARTITIONED);
        TStream<ChannelAndSequence> cs = pb.transform(beaconTupleChannelSeqTransformer(),
                ChannelAndSequence.class);
        TStream<ChannelAndSequence> joined = cs.unparallel();

        TStream<String> valid_count = joined.transform(partitionCounter(count),
                String.class);

        Tester tester = topology.getTester();
        Condition expectedCount = tester.tupleCount(valid_count, 1);
        Condition validCount = tester.stringContents(valid_count, "5");

         StreamsContextFactory.getStreamsContext(StreamsContext.Type.STANDALONE_TESTER).submit(topology).get();

         assertTrue(expectedCount.valid());
         assertTrue(validCount.valid());
    }
    
    @SuppressWarnings("serial")
    static Function<Integer, String> uniqueIdentifierMap(final int count) {
        return new Function<Integer, String>() {
            final HashMap<Integer, Integer> hashmap = new HashMap<Integer, Integer>();
            int inner_count = 0;

            @Override
            public String apply(Integer v) {
                Integer cnt = hashmap.get(v);
                if (cnt == null) {
                    hashmap.put(v, 1);
                } else {
                    hashmap.put(v, cnt + 1);
                }

                inner_count += 1;
                // Ensures that the number of tuples sent (count) are equal to
                // The number of tuples received (inner_count)
                if (inner_count >= count) {
                    return Integer.toString(hashmap.size());
                }
                return null;
            }

        };
    }

    @SuppressWarnings("serial")
    private static Function<ChannelAndSequence, String> partitionCounter(
            final int count) {
        return new Function<ChannelAndSequence, String>() {
            // A map where the channel is the key, and the map of sequence
            // counts
            // is the value.
            final HashMap<Integer, HashMap<Integer, Integer>> mapToCounts = new HashMap<Integer, HashMap<Integer, Integer>>();
            int inner_count = 0;

            @Override
            public String apply(ChannelAndSequence v) {
                Integer seq = v.getSequence();
                Integer channel = v.getChannel();
                HashMap<Integer, Integer> ch = mapToCounts.get(channel);
                if (ch == null) {
                    mapToCounts.put(channel, new HashMap<Integer, Integer>());
                    ch = mapToCounts.get(channel);
                }

                Integer seqCount = ch.get(seq);
                if (seqCount == null) {
                    ch.put(seq, 1);
                } else {
                    ch.put(seq, seqCount + 1);
                }
                inner_count += 1;
                if (inner_count >= count * 5){
                    for(Integer cha : mapToCounts.keySet()){
                        Map<Integer, Integer> counts = mapToCounts.get(cha);
                        for(Integer se : counts.keySet()){
                            if(counts.get(se) != 5){
                                throw new IllegalStateException("Invalid count for sequence " 
                                        + se + ". Should have a count of 5. Count is " 
                                        + counts.get(se));
                            }
                        }
                    }
                    return Integer.toString(mapToCounts.size());
                }
                return null;
            }
        };
    }

    @SuppressWarnings("serial")
    static Supplier<Iterable<KeyableBeaconTuple>> keyableBeacon5Counter(
            final int count) {
        return new Supplier<Iterable<KeyableBeaconTuple>>() {

            @Override
            public Iterable<KeyableBeaconTuple> get() {
                ArrayList<KeyableBeaconTuple> ret = new ArrayList<KeyableBeaconTuple>();
                for (int i = 0; i < count; i++) {
                    // Send 5 KeyableBeaconTuples with the same iteration count
                    // as a key. We then test that all BeaconTuples with the
                    // same
                    // key are sent to the same partition.
                    for (int j = 0; j < 5; j++) {
                        ret.add(new KeyableBeaconTuple(new BeaconTuple(i)));
                    }
                }
                // TODO Auto-generated method stub
                return ret;
            }

        };
    }

    @SuppressWarnings("serial")
    static Supplier<Iterable<BeaconTuple>> beaconTuple5Counter(
            final int count) {
        return new Supplier<Iterable<BeaconTuple>>() {

            @Override
            public Iterable<BeaconTuple> get() {
                List<BeaconTuple> ret = new ArrayList<BeaconTuple>();
                for (int i = 0; i < count; i++) {
                    // Send 5 BeaconTuples with the same iteration count
                    // as a key. We then test that all BeaconTuples with the
                    // same
                    // key are sent to the same partition.
                    for (int j = 0; j < 5; j++) {
                        ret.add(new BeaconTuple(i));
                    }
                }
                // TODO Auto-generated method stub
                return ret;
            }

        };
    }
    
    @SuppressWarnings("serial")
    static Function<KeyableBeaconTuple, ChannelAndSequence> channelSeqTransformer() {
        return new Function<KeyableBeaconTuple, ChannelAndSequence>() {
            int channel = -1;

            @Override
            public ChannelAndSequence apply(KeyableBeaconTuple v) {
                if (channel == -1) {
                    channel = PERuntime.getCurrentContext().getChannel();
                }
                // TODO Auto-generated method stub
                return new ChannelAndSequence(channel, (int) v.getTup()
                        .getSequence());
            }
        };
    }
    
    @SuppressWarnings("serial")
    static Function<BeaconTuple, ChannelAndSequence> beaconTupleChannelSeqTransformer() {
        return new Function<BeaconTuple, ChannelAndSequence>() {
            int channel = -1;

            @Override
            public ChannelAndSequence apply(BeaconTuple v) {
               
                if (channel == -1) {
                    channel = PERuntime.getCurrentContext().getChannel();
                }
                // TODO Auto-generated method stub
                return new ChannelAndSequence(channel, (int)(v.getSequence()));
            }

        };
    }

    @SuppressWarnings("serial")
    static Function<BeaconTuple, Integer> randomHashProducer() {
        return new Function<BeaconTuple, Integer>() {
            int channel = -1;

            @Override
            public Integer apply(BeaconTuple v) {
                if (channel == -1) {
                    channel = PERuntime.getCurrentContext().getChannel();
                }
                // TODO Auto-generated method stub
                return channel;
            }

        };
    }
}
