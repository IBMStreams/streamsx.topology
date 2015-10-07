/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;
import static com.ibm.streamsx.topology.logic.Value.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Initializable;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.logic.Value;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

public class ParallelTest extends TestTopology {
    @Test(expected=IllegalStateException.class)
    public void fanoutEndParallelException() throws Exception {
        checkUdpSupported();
        Topology topology = new Topology("testFanout");
        TStream<String> fanOut = topology.strings("hello").parallel(5)
                .filter(new AllowAll<String>());
        fanOut.print();
        fanOut.endParallel();
        
        Tester tester = topology.getTester();
        
	Condition<Long> expectedCount = tester.tupleCount(fanOut, 1);

	complete(tester, expectedCount, 60, TimeUnit.SECONDS);
	assertTrue(expectedCount.valid());
    }

    @Test
    public void testAdjacentParallel() throws Exception {
        checkUdpSupported();
        
        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = new Topology("testAdj");

        TStream<String> out0 = topology.strings(stringArray).parallel(of(20),
                TStream.Routing.HASH_PARTITIONED);
        out0 = out0.transform(randomStringProducer("region1")).endParallel();

        TStream<String> out2 = out0.parallel(of(5),
                TStream.Routing.HASH_PARTITIONED);
        out2 = out2.transform(randomStringProducer("region2")).endParallel();

        TStream<String> numRegions = out2.multiTransform(uniqueStringCounter(800,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished = tester
                .stringContentsUnordered(numRegions, "20", "5");

        Condition<Long> expectedCount = tester.tupleCount(out2, 800);

	complete(tester, assertFinished, 60, TimeUnit.SECONDS);

	System.out.println(expectedCount.getResult());
        assertTrue(expectedCount.valid());
	assertTrue(assertFinished.valid());
    }
    
    @Test
    public void testAdjacentEndParallelUnionSource() throws Exception {
        checkUdpSupported();
        
        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = new Topology("testAdj");

        TStream<String> out0 = topology.strings(stringArray).parallel(of(20),
                TStream.Routing.HASH_PARTITIONED);
        out0 = out0.transform(randomStringProducer("region1")).endParallel();
	
        TStream<String> out2 = topology.strings(stringArray).union(out0).parallel(of(5),
                TStream.Routing.HASH_PARTITIONED);
        out2 = out2.transform(randomStringProducer("region2")).endParallel();

        TStream<String> numRegions = out2.multiTransform(uniqueStringCounter(1600,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished = tester
                .stringContentsUnordered(numRegions, "20", "5");

        Condition<Long> expectedCount = tester.tupleCount(out2, 1600);

	complete(tester, assertFinished, 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
	assertTrue(assertFinished.valid());
    }

    @Test
    public void testParallelNonPartitioned() throws Exception {
        checkUdpSupported();

        Topology topology = new Topology("testParallel");
        final int count = new Random().nextInt(1000) + 37;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(5);

        TStream<Integer> is = pb.transform(randomHashProducer());
        TStream<Integer> joined = is.endParallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, "5");

	complete(tester, regionCount, 10, TimeUnit.SECONDS);
        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @Test
    public void testParallelWidthSupplier() throws Exception {
        checkUdpSupported();

        Topology topology = new Topology("testParallelWidthValue");
        final int count = new Random().nextInt(1000) + 37;
        String submissionWidthName = "width";
        final Integer submissionWidth = 5;
        
        @SuppressWarnings("serial")
        Supplier<Integer> supplier = new Supplier<Integer>() {
            
            @Override
            public Integer get() {
                return submissionWidth;
            }
        };

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(supplier);

        TStream<Integer> is = pb.transform(randomHashProducer());
        TStream<Integer> joined = is.endParallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, submissionWidth.toString());

        Map<String,Object> params = new HashMap<>();
        params.put(submissionWidthName, submissionWidth);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);

	complete(tester, regionCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @Test
    public void testParallelSubmissionParam() throws Exception {
        checkUdpSupported();

        Topology topology = new Topology("testParallelSubmissionParam");
        final int count = new Random().nextInt(1000) + 37;
        String submissionWidthName = "width";
        Integer submissionWidth = 5;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(
                topology.createSubmissionParameter(submissionWidthName, Integer.class));

        TStream<Integer> is = pb.transform(randomHashProducer());
        TStream<Integer> joined = is.endParallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, submissionWidth.toString());

        Map<String,Object> params = new HashMap<>();
        params.put(submissionWidthName, submissionWidth);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);

	complete(tester, regionCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @Test
    public void testParallelSubmissionParamInner() throws Exception {
        checkUdpSupported();

        Topology topology = new Topology("testParallelSubmissionParamInner");
        final int count = new Random().nextInt(1000) + 37;
        String submissionWidthName = "width";
        Integer submissionWidth = 5;
        String submissionAppendName = "append";
        boolean submissionAppend = true;
        String submissionFlushName = "flush";
        Integer submissionFlush = 1;
        String submissionThresholdName = "threshold";
        String submissionDefaultedThresholdName = "defaultedTreshold";
        Integer submissionThreshold = -1;
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);

        Supplier<Integer> threshold = topology.createSubmissionParameter(submissionThresholdName, Integer.class);
        Supplier<Integer> defaultedThreshold = topology.createSubmissionParameter(submissionDefaultedThresholdName, submissionThreshold);
                
        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(
                topology.createSubmissionParameter(submissionWidthName, Integer.class));

        TStream<Integer> is = pb.transform(randomHashProducer());
        
        // submission param use within a parallel region
        StreamSchema schema = getStreamSchema("tuple<int32 i>");
        SPLStream splStream = SPLStreams.convertStream(is, cvtMsgFunc(), schema);
        File tmpFile = File.createTempFile("parallelTest", null);
        tmpFile.deleteOnExit();
        Map<String,Object> splParams = new HashMap<>();
        splParams.put("file", tmpFile.getAbsolutePath());
        splParams.put("append", topology.createSubmissionParameter(submissionAppendName, submissionAppend));
        splParams.put("flush", SPL.createSubmissionParameter(topology, submissionFlushName, SPL.createValue(0, Type.MetaType.UINT32), false));
        SPL.invokeSink("spl.adapter::FileSink", splStream, splParams);
        
        // use a submission parameter in "inner" functional logic
        is = is.filter(thresholdFilter(threshold));
        is = is.filter(thresholdFilter(defaultedThreshold));

        // avoid another parallel impl limitation noted in issue#173
        is = is.filter(passthru());
        
        TStream<Integer> joined = is.endParallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, submissionWidth.toString());

        Map<String,Object> params = new HashMap<>();
        params.put(submissionWidthName, submissionWidth);
        params.put(submissionFlushName, submissionFlush);
        params.put(submissionThresholdName, submissionThreshold);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
	
	complete(tester, regionCount, 10, TimeUnit.SECONDS);
	
        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @SuppressWarnings("serial")
    private static Predicate<Integer> thresholdFilter(final Supplier<Integer> threshold) {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer tuple) {
                return tuple > threshold.get();
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static Predicate<Integer> passthru() {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer tuple) {
                return true;
            }
        };
    }
    
    private static BiFunction<Integer,OutputTuple,OutputTuple> cvtMsgFunc()
    {
        return new BiFunction<Integer,OutputTuple,OutputTuple>() {
            private static final long serialVersionUID = 1L;

            @Override
            public OutputTuple apply(Integer v1, OutputTuple v2) {
                v2.setInt("i", v1);
                return v2;
            }
        };
    }


    @Test
    public void testParallelSubmissionParamDefault() throws Exception {
        checkUdpSupported();

        Topology topology = new Topology("testParallelSubmissionParamDefault");
        final int count = new Random().nextInt(1000) + 37;
        String submissionWidthName = "width";
        Integer submissionWidth = 5;

        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(
                topology.createSubmissionParameter(submissionWidthName, submissionWidth));

        TStream<Integer> is = pb.transform(randomHashProducer());
        TStream<Integer> joined = is.endParallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, submissionWidth.toString());

	complete(tester, regionCount, 10, TimeUnit.SECONDS);
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

        TStream<BeaconTuple> kb = topology.source(
                keyableBeacon5Counter(count));
        TStream<BeaconTuple> pb = kb.parallel(new Value<Integer>(5), keyBeacon());
        TStream<ChannelAndSequence> cs = pb.transform(channelSeqTransformer());
        TStream<ChannelAndSequence> joined = cs.endParallel();

        TStream<String> valid_count = joined.transform(partitionCounter(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(valid_count, 1);
        Condition<List<String>> validCount = tester.stringContents(valid_count, "5");
        
        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

         assertTrue(expectedCount.valid());
         assertTrue(validCount.valid());
    }
    
    static Function<BeaconTuple, Long> keyBeacon() {
        
        return new Function<BeaconTuple,Long>() {
           private static final long serialVersionUID = 1L;

            @Override
            public Long apply(BeaconTuple v) {
                return v.getSequence();
            }};
    }
    
    @Test
    public void testObjectHashPartition() throws Exception {
        checkUdpSupported();
        
        Topology topology = new Topology("testObjectHashPartition");
        final int count = new Random().nextInt(10) + 37;

        TStream<String> kb = topology.source(
                stringTuple5Counter(count));
        TStream<String> pb = kb.parallel(Value.of(5), TStream.Routing.HASH_PARTITIONED);
        TStream<ChannelAndSequence> cs = pb.transform(stringTupleChannelSeqTransformer());
        TStream<ChannelAndSequence> joined = cs.endParallel();

        TStream<String> valid_count = joined.transform(partitionCounter(count));

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
    static Supplier<Iterable<BeaconTuple>> keyableBeacon5Counter( 
            final int count) {
        return new Supplier<Iterable<BeaconTuple>>() {

            @Override
            public Iterable<BeaconTuple> get() {
                ArrayList<BeaconTuple> ret = new ArrayList<BeaconTuple>();
                for (int i = 0; i < count; i++) {
                    // Send 5 KeyableBeaconTuples with the same iteration count
                    // as a key. We then test that all BeaconTuples with the
                    // same
                    // key are sent to the same partition.
                    for (int j = 0; j < 5; j++) {
                        ret.add(new BeaconTuple(i));
                    }
                }
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
                return ret;
            }

        };
    }
    
    @SuppressWarnings("serial")
    static abstract class ChannelGetter<F,R> implements Function<F,R>, Initializable {
        int channel = -1;
        
        @Override
        public void initialize(FunctionContext functionContext)
                throws Exception {
            channel = functionContext.getChannel();         
        }
    }
    
    @SuppressWarnings("serial")
    static Function<BeaconTuple, ChannelAndSequence> channelSeqTransformer() {
        return new ChannelGetter<BeaconTuple, ChannelAndSequence>() {
            @Override
            public ChannelAndSequence apply(BeaconTuple v) {
                return new ChannelAndSequence(channel, (int) v.getSequence());
            }
        };
    }
    
    @SuppressWarnings("serial")
    static Function<String, ChannelAndSequence> stringTupleChannelSeqTransformer() {
        return new ChannelGetter<String, ChannelAndSequence>() {
            @Override
            public ChannelAndSequence apply(String v) {
                return new ChannelAndSequence(channel, Integer.parseInt(v));
            }

        };
    }

    @SuppressWarnings("serial")
    static Function<BeaconTuple, Integer> randomHashProducer() {
        return new ChannelGetter<BeaconTuple, Integer>() {
            @Override
            public Integer apply(BeaconTuple v) {
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
        
        // workaround: avoid union().endParallel() bug  issue#127
        splitChFanin = splitChFanin.filter(new AllowAll<String>());

        /////////////////////////////////////
        TStream<String> all = splitChFanin.endParallel();
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
        stringsP = stringsP.endParallel();
        
        Tester tester = topology.getTester();
        
        Condition<Long> fiveTuples = tester.tupleCount(stringsP, 5);
        
        Condition<List<String>> contents = tester.stringContentsUnordered(stringsP, "A", "B", "C", "D", "E");
        
        complete(tester, fiveTuples, 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
    }
    
    @Test
    public void testUnionUnparallel() throws Exception {
        Topology topology = new Topology();
        
        TStream<String> strings = topology.strings("A", "B", "C", "D", "E");
        TStream<String> stringsP = strings.parallel(3);
        TStream<String> stringsP_AB = stringsP.filter(allowAB());
        TStream<String> stringsP_CDE = stringsP.filter(allowCDE());
        
        stringsP = stringsP_AB.union(stringsP_CDE).endParallel();
        
        Tester tester = topology.getTester();
        
        Condition<Long> fiveTuples = tester.tupleCount(stringsP, 5);
        
        Condition<List<String>> contents = tester.stringContentsUnordered(stringsP, "A", "B", "C", "D", "E");
        
        complete(tester, fiveTuples, 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
    }
    
    @Test
    public void testParallelIsolate() throws Exception {
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
      
        Topology topology = new Topology();
        
        TStream<String> strings = topology.strings("A", "B", "C", "D", "E", "F", "G", "H", "I");
        TStream<String> stringsP = strings.parallel(3);
        TStream<Map<Integer,String>> channelPe = stringsP.transform(new ChannelAndPEid());
        channelPe = channelPe.endParallel();
        
        TStream<String> result =  channelPe.transform(new CheckSeparatePE());
        
        Tester tester = topology.getTester();
        
        Condition<Long> singleResult = tester.tupleCount(result, 1);
        
        Condition<List<String>> contents = tester.stringContents(result, "true");
        
        complete(tester, singleResult, 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
    }
    
    @SuppressWarnings("serial")
    public static Predicate<String> nopStringFilter(){
	return new Predicate<String>(){
	    @Override
	    public boolean test(String tuple){
		return true;
	    }
	};
    }

    @SuppressWarnings("serial")
    public static Predicate<String> allowAB(){
        return new Predicate<String>() {
            
            @Override
            public boolean test(String tuple) {
                if(tuple.equals("A") || tuple.equals("B")){
                    return true;
                }
                return false;
            }
        };
    }
    
    @SuppressWarnings("serial")
    public static Predicate<String> allowCDE(){
        return new Predicate<String>() {
            
            @Override
            public boolean test(String tuple) {
                if(tuple.equals("C") || tuple.equals("D") || tuple.equals("E")){
                    return true;
                }
                return false;
            }
        };
    }
    
    public static class CheckSeparatePE implements Function<Map<Integer,String>, String> {
        
        private static final long serialVersionUID = 1L;
        private final Map<Integer, String> seen = new HashMap<>();
        private boolean ok;

        @Override
        public String apply(Map<Integer, String> v) {
            seen.putAll(v);
            if (ok || seen.size() != 3)
                return null;
            
            Set<String> pes = new HashSet<>();
            pes.addAll(seen.values());
            if (pes.size() != 3)
                return Boolean.FALSE.toString();
            
            ok = true;
            return Boolean.TRUE.toString();
        }
        
    }
    
    public static class ChannelAndPEid implements Function<String,Map<Integer,String>>, Initializable {
        private static final long serialVersionUID = 1L;
        private FunctionContext functionContext;

        @Override
        public Map<Integer, String> apply(String v) {
            return Collections.singletonMap(
                    functionContext.getChannel(),
                    functionContext.getContainer().getId());
        }

        @Override
        public void initialize(FunctionContext functionContext)
                throws Exception {
            
            this.functionContext = functionContext; 
        }
        
    }
    
    public static List<String> getListOfUniqueStrings(int num){
        List<String> l = new ArrayList<>();
        for(int i = 0; i < num; i++){
            l.add(Integer.toString(i));
        }
        return l;
    }
    
    @SuppressWarnings("serial")
    public static Function<String, String> randomStringProducer(final String region){
        return new Function<String, String>(){
            String uuid = null;
            @Override
            public String apply(String v) {
		if(uuid == null){
		    uuid=UUID.randomUUID().toString();
		}
		String ret = v + " " + region+uuid;                
                return ret;
            }
            
        };
    }
    
    @SuppressWarnings("serial")
    public static Function<String, Iterable<String>> uniqueStringCounter(final int count, final String region){
        return new Function<String, Iterable<String>>(){
            Set<String> numChannels1 = new HashSet<>();
            Set<String> numChannels2 = new HashSet<>();
            int _count = 0;
            @Override
            public Iterable<String> apply(String v) {
                if(_count < count){
                    _count++;                
                    String[] channelIds = v.split(" ");
		    if(channelIds.length > 2){
			numChannels1.add(channelIds[1]);
			numChannels2.add(channelIds[2]);
		    }
		    if(_count == count){
                        List<String> l = new ArrayList<String>();
			
                        l.add(Integer.toString(numChannels1.size()));
                        l.add(Integer.toString(numChannels2.size()));
                        return l;
                    }
                }
                return null;  
            }
            
        };
    }
    
    
}
