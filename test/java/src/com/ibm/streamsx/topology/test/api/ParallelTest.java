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

import org.junit.Test;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TStream.Routing;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
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
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tuple.BeaconTuple;

/**
 * Parallel tests.
 * testParallelNonPartitioned
 * testParallelWidthSupplier
 * testAdjacentParallel* - S -> [ R1 ] -> [ R1 ] > E
 * 
 *
 */
public class ParallelTest extends TestTopology {
    @Test
    public void testMultipleParallelUse() throws Exception {
        Topology topology = newTopology("testFanout");
        TStream<String> sp = topology.strings("A", "B", "C", "D", "E").parallel(2);
        
        TStream<String> fanOut = sp.modify(v ->v.concat("Y"));
        fanOut.forEach(v -> {});
              
        sp = sp.modify(v -> v.concat("X"));
        sp = sp.union(fanOut);
        
        fanOut = fanOut.endParallel();
        
        sp.forEach(v -> {});
        sp = sp.endParallel();

        Tester tester = topology.getTester();

        Condition<Long> fanOutN = tester.tupleCount(fanOut, 5);
        Condition<Long> spN = tester.tupleCount(sp, 10);
        
        Condition<List<String>> fanOutC = tester.stringContentsUnordered(fanOut, "AY", "BY", "CY", "DY", "EY");
        Condition<List<String>> spC = tester.stringContentsUnordered(sp, "AY", "BY", "CY", "DY", "EY", "AX", "BX", "CX", "DX", "EX");     

        complete(tester, Condition.all(fanOutN, spN, fanOutC, spC), 60, TimeUnit.SECONDS);
        assertTrue(fanOutN.valid());
        assertTrue(spN.valid());
        assertTrue(fanOutC.valid());
        assertTrue(spC.valid());
    }

    private void testAdjacentParallel(Routing routing1, Routing routing2) throws Exception {
        checkUdpSupported();
        
        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = newTopology();

        TStream<String> out0 = topology.strings(stringArray).parallel(of(20),
                routing1);
        out0 = out0.map(randomStringProducer("region1")).endParallel();

        TStream<String> out2 = out0.parallel(of(5),
                routing2);
        out2 = out2.map(randomStringProducer("region2")).endParallel();

        TStream<String> numRegions = out2.flatMap(uniqueStringCounter(800,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished = tester.stringContentsUnordered(numRegions, "20", "5");

        Condition<Long> expectedCount = tester.tupleCount(out2, 800);
       

        complete(tester, allConditions(assertFinished, expectedCount), 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(assertFinished.valid());
    }

    @Test
    public void testAdjacentHashPartitionedParallel() throws Exception {
        testAdjacentParallel(Routing.HASH_PARTITIONED, Routing.HASH_PARTITIONED);
    }

    @Test
    public void testAdjacentRoundRobinParallel() throws Exception {
        testAdjacentParallel(Routing.ROUND_ROBIN, Routing.ROUND_ROBIN);
    }

    @Test
    public void testAdjacentRoundRobinHashPartitionedParallel() throws Exception {
        testAdjacentParallel(Routing.ROUND_ROBIN, Routing.HASH_PARTITIONED);
    }

    @Test
    public void testAdjacentHashPartitionedRoundRobinParallel() throws Exception {
        testAdjacentParallel(Routing.HASH_PARTITIONED, Routing.ROUND_ROBIN);
    }

    @SuppressWarnings("serial")
    private static class ChannelFilter implements Predicate<String>, Initializable {
        int channel = -1;
        int nChannels = -1;

        @Override
        public void initialize(FunctionContext functionContext)
                throws Exception {
            channel = functionContext.getChannel();
            nChannels = functionContext.getMaxChannels();
        }

        @Override
        public boolean test(String s) {
            if (s.hashCode() >= 0) {
                return s.hashCode() % nChannels == channel;
            } else {
                return (s.hashCode() & 0x00000000FFFFFFFFL) % nChannels == channel;
            }
        }
    }

    @Test
    public void testAdjacentKeyPartitionedParallel() throws Exception {
        checkUdpSupported();

        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = newTopology();

        TStream<String> out1 = topology.strings(stringArray)
                                       .parallel(of(3), String::hashCode);
        out1 = out1.map(randomStringProducer("region1")).endParallel();

        TStream<String> out2 = out1.parallel(of(5), String::hashCode);
        out2 = out2.filter(new ChannelFilter())
                   .map(randomStringProducer("region2")).endParallel();

        TStream<String> numRegions = out2.flatMap(uniqueStringCounter(800,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished = tester.stringContentsUnordered(numRegions, "3", "5");

        Condition<Long> expectedCount = tester.tupleCount(out2, 800);


        complete(tester, allConditions(assertFinished, expectedCount), 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(assertFinished.valid());
    }

    @Test
    public void testAdjacentEndParallelUnionSource() throws Exception {
        checkUdpSupported();
        
        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = newTopology();

        TStream<String> out0 = topology.strings(stringArray).parallel(of(20),
                TStream.Routing.HASH_PARTITIONED);
        out0 = out0.map(randomStringProducer("region1")).endParallel();
	
        TStream<String> out2 = topology.strings(stringArray).union(out0).parallel(of(5),
                TStream.Routing.HASH_PARTITIONED);
        out2 = out2.map(randomStringProducer("region2")).endParallel();

        TStream<String> numRegions = out2.flatMap(uniqueStringCounter(1600,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished = tester
                .stringContentsUnordered(numRegions, "20", "5");

        Condition<Long> expectedCount = tester.tupleCount(out2, 1600);

	complete(tester, allConditions(assertFinished, expectedCount), 60, TimeUnit.SECONDS);

        assertTrue(expectedCount.getResult().toString(), expectedCount.valid());
	assertTrue(assertFinished.getResult().toString(), assertFinished.valid());
    }

    @Test
    public void testMultiAdjacentPartitionedParallelChildren() throws Exception {
        checkUdpSupported();

        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = newTopology();


        TStream<String> out1 = topology.strings(stringArray)
                                       .parallel(of(3), String::hashCode);
        out1 = out1.map(randomStringProducer("region1")).endParallel();

        TStream<String> out2 = out1.parallel(of(5), String::hashCode);
        out2 = out2.filter(new ChannelFilter())
                   .map(randomStringProducer("region2")).endParallel();

        TStream<String> out3 = out1.parallel(of(2), String::hashCode);
        out3 = out3.filter(new ChannelFilter())
                   .map(randomStringProducer("region3")).endParallel();

        TStream<String> numRegions2 = out2.flatMap(uniqueStringCounter(800,
                "region"));

        TStream<String> numRegions3 = out3.flatMap(uniqueStringCounter(800,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished2 = tester.stringContentsUnordered(numRegions2, "3", "5");
        Condition<List<String>> assertFinished3 = tester.stringContentsUnordered(numRegions3, "3", "2");

        Condition<Long> expectedCount2 = tester.tupleCount(out2, 800);
        Condition<Long> expectedCount3 = tester.tupleCount(out3, 800);

        complete(tester,
                allConditions(assertFinished2, expectedCount2,
                        assertFinished3, expectedCount3),
                60,
                TimeUnit.SECONDS);

        assertTrue(expectedCount2.valid());
        assertTrue(expectedCount3.valid());
        assertTrue(assertFinished2.valid());
        assertTrue(assertFinished3.valid());
    }

    @Test
    public void testMultiAdjacentMixedParallelChildren1() throws Exception {
        checkUdpSupported();

        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = newTopology();


        TStream<String> out1 = topology.strings(stringArray)
                                       .parallel(of(3), String::hashCode);
        out1 = out1.map(randomStringProducer("region1")).endParallel();

        TStream<String> out2 = out1.parallel(of(5), String::hashCode);
        out2 = out2.filter(new ChannelFilter())
                   .map(randomStringProducer("region2")).endParallel();

        TStream<String> out3 = out1.parallel(of(2), String::hashCode);
        out3 = out3.filter(new ChannelFilter())
                   .map(randomStringProducer("region3")).endParallel();

        TStream<String> out4 = out1.map(randomStringProducer("region4"));

        TStream<String> numRegions2 = out2.flatMap(uniqueStringCounter(800,
                "region"));

        TStream<String> numRegions3 = out3.flatMap(uniqueStringCounter(800,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished2 = tester.stringContentsUnordered(numRegions2, "3", "5");
        Condition<List<String>> assertFinished3 = tester.stringContentsUnordered(numRegions3, "3", "2");

        Condition<Long> expectedCount2 = tester.tupleCount(out2, 800);
        Condition<Long> expectedCount3 = tester.tupleCount(out3, 800);
        Condition<Long> expectedCount4 = tester.tupleCount(out4, 800);


        complete(tester,
                allConditions(assertFinished2, expectedCount2,
                        assertFinished3, expectedCount3,
                        expectedCount4),
                60,
                TimeUnit.SECONDS);

        assertTrue(expectedCount2.valid());
        assertTrue(expectedCount3.valid());
        assertTrue(expectedCount4.valid());
        assertTrue(assertFinished2.valid());
        assertTrue(assertFinished3.valid());
    }


    @Test
    public void testMultiAdjacentMixedParallelChildren2() throws Exception {
        checkUdpSupported();

        List<String> stringList = getListOfUniqueStrings(800);
        String stringArray[] = new String[800];
        stringArray = stringList.toArray(stringArray);
        Topology topology = newTopology();


        TStream<String> out1 = topology.strings(stringArray)
                                       .parallel(of(3), String::hashCode);
        out1 = out1.map(randomStringProducer("region1")).endParallel();

        TStream<String> out2 = out1.parallel(of(5), String::hashCode);
        out2 = out2.filter(new ChannelFilter())
                   .map(randomStringProducer("region2")).endParallel();

        TStream<String> out3 = out1.parallel(of(2), String::hashCode);
        out3 = out3.filter(new ChannelFilter())
                   .map(randomStringProducer("region3")).endParallel();

        TStream<String> out4 = out1.map(randomStringProducer("region4"));
        TStream<String> out5 = out1.map(randomStringProducer("region5"));

        TStream<String> numRegions2 = out2.flatMap(uniqueStringCounter(800,
                "region"));

        TStream<String> numRegions3 = out3.flatMap(uniqueStringCounter(800,
                "region"));

        Tester tester = topology.getTester();

        Condition<List<String>> assertFinished2 = tester.stringContentsUnordered(numRegions2, "3", "5");
        Condition<List<String>> assertFinished3 = tester.stringContentsUnordered(numRegions3, "3", "2");

        Condition<Long> expectedCount2 = tester.tupleCount(out2, 800);
        Condition<Long> expectedCount3 = tester.tupleCount(out3, 800);
        Condition<Long> expectedCount4 = tester.tupleCount(out4, 800);
        Condition<Long> expectedCount5 = tester.tupleCount(out5, 800);

        complete(tester,
                allConditions(assertFinished2, expectedCount2,
                        assertFinished3, expectedCount3,
                        expectedCount4,
                        expectedCount5),
                60,
                TimeUnit.SECONDS);

        assertTrue(expectedCount2.valid());
        assertTrue(expectedCount3.valid());
        assertTrue(expectedCount4.valid());
        assertTrue(expectedCount5.valid());
        assertTrue(assertFinished2.valid());
        assertTrue(assertFinished3.valid());
    }


    @Test
    public void testParallelNonPartitioned() throws Exception {
        checkUdpSupported();

        Topology topology = newTopology("testParallel");
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

	complete(tester, allConditions(regionCount, expectedCount), 10, TimeUnit.SECONDS);
        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @Test
    public void testParallelWidthSupplier() throws Exception {
        checkUdpSupported();

        Topology topology = newTopology("testParallelWidthValue");
        final int count = new Random().nextInt(1000) + 37;
        String submissionWidthName = "width";
        final Integer submissionWidth = 5;
        
        TStream<BeaconTuple> fb = BeaconStreams.beacon(topology, count);
        TStream<BeaconTuple> pb = fb.parallel(() -> submissionWidth);

        TStream<Integer> is = pb.transform(randomHashProducer());
        TStream<Integer> joined = is.endParallel();
        TStream<String> numRegions = joined.transform(
                uniqueIdentifierMap(count));

        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(numRegions, 1);
        Condition<List<String>> regionCount = tester.stringContents(numRegions, submissionWidth.toString());

        complete(tester, allConditions(regionCount, expectedCount), 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @Test
    public void testParallelSubmissionParam() throws Exception {
        checkUdpSupported();

        Topology topology = newTopology("testParallelSubmissionParam");
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

        complete(tester, allConditions(regionCount, regionCount), 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.valid());
        assertTrue(regionCount.valid());
    }

    @Test
    public void testParallelSubmissionParamInner() throws Exception {
        checkUdpSupported();

        Topology topology = newTopology("testParallelSubmissionParamInner");
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
	
        complete(tester, allConditions(expectedCount, regionCount), 10, TimeUnit.SECONDS);

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

        Topology topology = newTopology("testParallelSubmissionParamDefault");
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

        complete(tester, allConditions(regionCount, expectedCount) , 10, TimeUnit.SECONDS);
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
               
        Topology topology = newTopology("testParallelPartition");
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
        
        complete(tester, allConditions(expectedCount, validCount), 10, TimeUnit.SECONDS);

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
        
        Topology topology = newTopology("testObjectHashPartition");
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
        
        complete(tester, allConditions(expectedCount, validCount), 10, TimeUnit.SECONDS);

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
        
        final Topology topology = newTopology("testParallelSplit");
        
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

        complete(tester, allConditions(uCount, contents), 10, TimeUnit.SECONDS);

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

    /**
     * Tests the input to a region also being consumed
     * by a non-parallel operator.
     */
    @Test
    public void testParallelPreFanOut() throws Exception {
        Topology topology = newTopology();
        
        TStream<String> strings = topology.strings("A", "B", "C", "D", "E");
        TStream<String> pre = strings.modify(s -> s.concat("X"));
        TStream<String> stringsP = strings.parallel(3);
        stringsP = stringsP.filter(new AllowAll<String>());
        stringsP = stringsP.endParallel();
        
        Tester tester = topology.getTester();
        
        Condition<Long> fiveTuples = tester.tupleCount(stringsP, 5);
        Condition<List<String>> preContents = tester.stringContents(pre, "AX", "BX", "CX", "DX", "EX");
        Condition<List<String>> contents = tester.stringContentsUnordered(stringsP, "A", "B", "C", "D", "E");
        
        complete(tester, allConditions(fiveTuples, contents, preContents), 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
        assertTrue(preContents.valid());
    }
    
    @Test
    public void testUnionEndparallel() throws Exception {
        checkUdpSupported();
        
        Topology topology = newTopology();
        
        TStream<String> strings = topology.strings("A", "B", "C", "D", "E");
        TStream<String> stringsP = strings.parallel(3);
        TStream<String> stringsP_AB = stringsP.filter(allowAB()).modify(s->s.concat("X"));
        TStream<String> stringsP_CDE = stringsP.filter(allowCDE()).modify(s->s.concat("Y"));;
        
        stringsP = stringsP_AB.union(stringsP_CDE).endParallel();
        
        Tester tester = topology.getTester();
        
        Condition<List<String>> contents = tester.stringContentsUnordered(stringsP, "AX", "BX", "CY", "DY", "EY");
        
        complete(tester, contents, 10, TimeUnit.SECONDS);

        assertTrue("contents: "+contents, contents.valid());
    }
    
    /**
     * Test union->parallel
     */
    @Test
    public void testUnionParallel() throws Exception {
        checkUdpSupported();
        
        Topology topology = newTopology();
        
        TStream<String> s1 = topology.strings("A", "B", "C", "D", "E");
        TStream<String> s2 = topology.strings("W", "X", "Y", "Z");
        TStream<String> s = s1.union(s2);
        TStream<String> sp = s.parallel(3).modify(v->v.concat("P")).endParallel();
        s = s.modify(v->v.concat("U"));
        s.print();
        
        Tester tester = topology.getTester();
        
        Condition<List<String>> spc = tester.stringContentsUnordered(sp,
                "AP", "BP", "CP", "DP", "EP", "WP", "XP", "YP", "ZP");
        Condition<List<String>> sc = tester.stringContentsUnordered(s,
                "AU", "BU", "CU", "DU", "EU", "WU", "XU", "YU", "ZU");
        
        complete(tester, allConditions(spc, sc), 10, TimeUnit.SECONDS);

        assertTrue(spc.valid());
        assertTrue(sc.valid());
    }
    
    @Test
    public void testBroadcast() throws Exception {
        checkUdpSupported();
        
        Topology topology = newTopology();
        
        TStream<String> strings = topology.strings("1", "7", "19", "23", "57");
        TStream<String> stringsP = strings.parallel(()->3, Routing.BROADCAST);
        TStream<ChannelAndSequence> withChannel = stringsP.map(stringTupleChannelSeqTransformer());    
        withChannel = withChannel.asType(ChannelAndSequence.class).endParallel();
        TStream<String> result = StringStreams.toString(withChannel);
        
        Tester tester = topology.getTester();
        
        Condition<Long> fifeteenTuples = tester.tupleCount(result, 15);
        
        Condition<List<String>> contents = tester.stringContentsUnordered(result,
                "CS:0:1", "CS:0:7", "CS:0:19", "CS:0:23", "CS:0:57",
                "CS:1:1", "CS:1:7", "CS:1:19", "CS:1:23", "CS:1:57",
                "CS:2:1", "CS:2:7", "CS:2:19", "CS:2:23", "CS:2:57");
        
        this.getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        complete(tester, allConditions(fifeteenTuples, contents), 10, TimeUnit.SECONDS);  
        
        assertTrue("broadcast count: ", fifeteenTuples.valid());
        assertTrue("broadcast contents: ", contents.valid());
    }
    
    public static class TT<T> implements Function<T,String> {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(T v) {
            System.err.println("TT:" + (v == null ? "NULL": v.getClass().getName()));
            try {
            return v.toString();
            } finally {
                System.err.println("TT:DONE");
            }
        }
        
    }
    
    @Test
    public void testParallelIsolate() throws Exception {
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);

        skipVersion("udp-fusing", 4, 2);
        
        Topology topology = newTopology();
        
        TStream<String> strings = topology.strings("A", "B", "C", "D", "E", "F", "G", "H", "I");
        TStream<String> stringsP = strings.parallel(3);
        TStream<Map<Integer,String>> channelPe = stringsP.transform(new ChannelAndPEid());
        channelPe = channelPe.endParallel();
        
        TStream<String> result =  channelPe.transform(new CheckSeparatePE());
        
        Tester tester = topology.getTester();
        
        Condition<Long> singleResult = tester.tupleCount(result, 1);
        
        Condition<List<String>> contents = tester.stringContents(result, "true");
        
        complete(tester, allConditions(singleResult,contents), 10, TimeUnit.SECONDS);

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
