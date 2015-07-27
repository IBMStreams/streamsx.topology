/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

public class ParallelTest extends TestTopology {
    @Test
    public void testParallelNonPartitioned() throws Exception {
        checkUdpSupported();

        Topology topology = new Topology("testParallel");
        final int count = new Random().nextInt(1000) + 37;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(5);

        TStream<Integer> is = pb.transform(randomHashProducer(), Integer.class);
        TStream<Integer> joined = is.unparallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count), String.class);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, "5");

        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();

        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }
    
    private void checkUdpSupported() {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER ||
                getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
    }
    
    @Test
    public void testParallelPartitioned() throws Exception {
        
        checkUdpSupported();
               
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
        Condition<Long> expectedCount = tester.tupleCount(valid_count, 1);
        Condition<List<String>> validCount = tester.stringContents(valid_count, "5");
        
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

         assertTrue(expectedCount.valid());
         assertTrue(validCount.valid());
    }
    
    @Test
    public void testObjectHashPartition() throws Exception {
        checkUdpSupported();
        
        Topology topology = new Topology("testObjectHashPartition");
        final int count = new Random().nextInt(10) + 37;

        TStream<String> kb = topology.source(
                stringTuple5Counter(count), String.class);
        TStream<String> pb = kb.parallel(5, TStream.Routing.PARTITIONED);
        TStream<ChannelAndSequence> cs = pb.transform(stringTupleChannelSeqTransformer(),
                ChannelAndSequence.class);
        TStream<ChannelAndSequence> joined = cs.unparallel();

        TStream<String> valid_count = joined.transform(partitionCounter(count),
                String.class);

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(valid_count, 1);
        Condition<List<String>> validCount = tester.stringContents(valid_count, "5");
        
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

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
    static Supplier<Iterable<String>> stringTuple5Counter(
            final int count) {
        return new Supplier<Iterable<String>>() {

            @Override
            public Iterable<String> get() {
                List<String> ret = new ArrayList<String>();
                for (int i = 0; i < count; i++) {
                    // Send 5 BeaconTuples with the same iteration count
                    // as a key. We then test that all BeaconTuples with the
                    // same
                    // key are sent to the same partition.
                    for (int j = 0; j < 5; j++) {
                        ret.add(Integer.toString(i));
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
    static Function<String, ChannelAndSequence> stringTupleChannelSeqTransformer() {
        return new Function<String, ChannelAndSequence>() {
            int channel = -1;

            @Override
            public ChannelAndSequence apply(String v) {
               
                if (channel == -1) {
                    channel = PERuntime.getCurrentContext().getChannel();
                }
                return new ChannelAndSequence(channel, Integer.parseInt(v));
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
    
    @Test
    public void testParallelSplit() throws Exception {
        // embedded: split works but validation fails because it
        // depends on validating the correct parallel channel too,
        // and in embedded mode PERuntime.getCurrentContext().getChannel()
        // returns -1.  issue#126
        // until that's addressed...
        checkUdpSupported();
        
        // parallel().split() is an interesting case because split()
        // has >1 oports.
        
        final Topology topology = new Topology("testParallelSplit");
        
        // Order the tuples based on their expected/required
        // delivery path given an n-ch round-robin parallel region
        // and our split() behavior
        int splitWidth = 3;
        int parallelWidth = 2;
        String[] strs = {
            "pch=0 sch=0", "pch=1 sch=0", 
            "pch=0 sch=1", "pch=1 sch=1", 
            "pch=0 sch=2", "pch=1 sch=2", 
            "pch=0 another-sch=2", "pch=1 another-sch=2",
            "pch=0 another-sch=1", "pch=1 another-sch=1", 
            "pch=0 another-sch=0", "pch=1 another-sch=0", 
            };
        String[] strsExpected = {
            "[pch=0, sch=0] pch=0 sch=0", "[pch=1, sch=0] pch=1 sch=0", 
            "[pch=0, sch=1] pch=0 sch=1", "[pch=1, sch=1] pch=1 sch=1", 
            "[pch=0, sch=2] pch=0 sch=2", "[pch=1, sch=2] pch=1 sch=2", 
            "[pch=0, sch=2] pch=0 another-sch=2", "[pch=1, sch=2] pch=1 another-sch=2",
            "[pch=0, sch=1] pch=0 another-sch=1", "[pch=1, sch=1] pch=1 another-sch=1", 
            "[pch=0, sch=0] pch=0 another-sch=0", "[pch=1, sch=0] pch=1 another-sch=0", 
            };
 
        TStream<String> s1 = topology.strings(strs);

        s1 = s1.parallel(parallelWidth);
        /////////////////////////////////////
        
        List<TStream<String>> splits = s1
                .split(splitWidth, myStringSplitter());

        assertEquals("list size", splitWidth, splits.size());

        List<TStream<String>> splitChResults = new ArrayList<>();
        for(int i = 0; i < splits.size(); i++) {
            splitChResults.add( splits.get(i).modify(parallelSplitModifier(i)) );
        }
        
        TStream<String> splitChFanin = splitChResults.get(0).union(
                        new HashSet<>(splitChResults.subList(1, splitChResults.size())));
        
        // workaround: avoid union().unparallel() bug  issue#127
        splitChFanin = splitChFanin.filter(new AllowAll<String>());

        /////////////////////////////////////
        TStream<String> all = splitChFanin.unparallel();
        all.print();

        Tester tester = topology.getTester();
        
        TStream<String> dupAll = all.filter(new AllowAll<String>());
        Condition<Long> uCount = tester.tupleCount(dupAll, strsExpected.length); 
        Condition<List<String>> contents = tester.stringContentsUnordered(dupAll, strsExpected);

        complete(tester, uCount, 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
    }
    
    @SuppressWarnings("serial")
    static UnaryOperator<String> parallelSplitModifier(final int splitCh) {
        return new UnaryOperator<String>() {

            @Override
            public String apply(String v) {
                OperatorContext oc = PERuntime.getCurrentContext();
                return String.format("[pch=%d, sch=%d] %s",
                        oc.getChannel(), splitCh, v.toString());
            }
        }; 
    }
    
    /**
     * Partition strings based on the last character of the string.
     * If the last character is a digit return its value as an int, else return -1.
     * @return
     */
    @SuppressWarnings("serial")
    private static ToIntFunction<String> myStringSplitter() {
        return new ToIntFunction<String>() {

            @Override
            public int applyAsInt(String s) {
                char ch = s.charAt(s.length() - 1);
                return Character.digit(ch, 10);
            }
        };
    }

    @Test
    @Ignore("Issue #131")
    public void testParallelPreFanOut() throws Exception {
        Topology topology = new Topology();
        
        TStream<String> strings = topology.strings("A", "B", "C", "D", "E");
        strings.print();
        TStream<String> stringsP = strings.parallel(3);
        stringsP = stringsP.filter(new AllowAll<String>());
        stringsP = stringsP.unparallel();
        
        Tester tester = topology.getTester();
        
        Condition<Long> fiveTuples = tester.tupleCount(stringsP, 5);
        
        Condition<List<String>> contents = tester.stringContentsUnordered(stringsP, "A", "B", "C", "D", "E");
        
        complete(tester, fiveTuples, 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
    }
}
