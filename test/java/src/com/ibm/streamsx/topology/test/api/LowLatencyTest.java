/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.logic.Logic.identity;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIdAppend;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIds;
import static com.ibm.streamsx.topology.test.api.PlaceableTest.adlAssertColocated;
import static com.ibm.streamsx.topology.test.api.PlaceableTest.adlAssertDefaultHostpool;
import static com.ibm.streamsx.topology.test.api.PlaceableTest.produceADL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.w3c.dom.Document;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class LowLatencyTest extends TestTopology {
    @Test
    public void testSimpleLowLatency() throws Exception{
        adlOk();
        
        Topology topology = newTopology();

        // Construct topology
        TStream<String> ss = topology.strings("hello");
        TStream<String> ss1 = ss.transform(identity()).invocationName("SS1").lowLatency();
        TStream<String> ss2 = ss1.transform(identity()).invocationName("SS2").endLowLatency();
        ss2.forEach(tuple->{});
        
        Document adl = produceADL(topology);
        adlAssertDefaultHostpool(adl);
        adlAssertColocated(adl, false, "SS1", "SS2");
    }
    
    @Test
    public void testMultipleRegionLowLatency() throws Exception{
        adlOk();
        
        Topology topology = newTopology();

        // Construct topology
        TStream<String> ss = topology.strings("hello")
                .map(identity()).map(identity());
        
        TStream<String> ss1 = ss.map(identity()).invocationName("R1_A").lowLatency();
        TStream<String> ss2 = ss1
                .map(identity()).invocationName("R1_B")
                .map(identity()).invocationName("R1_C")
                .endLowLatency().map(identity());
        
        TStream<String> ss3 = ss2.map(identity()).invocationName("R2_X").lowLatency();
        ss3.map(identity()).invocationName("R2_Y").map(identity()).invocationName("R2_Z")
            .endLowLatency().forEach(tuple->{});
        
        Document adl = produceADL(topology);
        adlAssertDefaultHostpool(adl);
        adlAssertColocated(adl, false, "R1_A", "R1_B", "R1_C");
        adlAssertColocated(adl, false, "R2_X", "R2_Y", "R2_Z");
    }
    
    @Test
    public void testUDPContainingLowLatency() throws Exception{
        adlOk();
        
        Topology topology = newTopology();

        // Construct topology
        TStream<String> ss = topology.strings("hello");
        ss = ss.parallel(3);
        TStream<String> ss1 = ss.map(identity()).invocationName("UDP_SS1").lowLatency();
        ss1 = ss1.map(identity()).invocationName("UDP_SS2");
        TStream<String> ss2 = ss1.map(identity()).invocationName("UDP_SS3").endLowLatency();
        ss2 = ss2.endParallel();
        ss2.forEach(tuple->{});
        
        Document adl = produceADL(topology);
        adlAssertDefaultHostpool(adl);
        adlAssertColocated(adl, true, "UDP_SS1", "UDP_SS2", "UDP_SS3");
    }
    @Test
    public void testUDPNextToLowLatency() throws Exception{
        adlOk();
        
        Topology topology = newTopology();

        // Construct topology
        TStream<String> ss = topology.strings("hello").invocationName("UDP_SRC");
        ss = ss.parallel(3);
        TStream<String> ss1 = ss.lowLatency();
        ss1 = ss1.map(identity()).invocationName("UDP_SS1");
        TStream<String> ss2 = ss1.map(identity()).invocationName("UDP_SS2").endLowLatency();
        ss2 = ss2.endParallel();
        ss2.forEach(tuple->{});
        
        Document adl = produceADL(topology);
        adlAssertDefaultHostpool(adl);
        adlAssertColocated(adl, true, "UDP_SS1", "UDP_SS2");
    }
    
    @Test
    public void testLowLatencySplit() throws Exception {
        // Uses Condition.getResult - not supported.
        assumeTrue(!isStreamingAnalyticsRun());
        
        // lowLatency().split() is an interesting case because split()
        // has >1 oports.
        
        final Topology topology = newTopology("testLowLatencySplit");
        
        int splitWidth = 3;
        String[] strs = {"ch0", "ch1", "ch2"};
        TStream<String> s1 = topology.strings(strs);

        s1 = s1.isolate();
        s1 = s1.lowLatency();
        /////////////////////////////////////
        
        // assume that if s1.modify and the split().[modify()] are
        // in the same PE, that s1.split() is in the same too
        TStream<String> s2 = s1.modify(unaryGetPEId()).endLowLatency();
        
        List<TStream<String>> splits = s1
                .split(splitWidth, roundRobinSplitter());

        List<TStream<String>> splitChResults = new ArrayList<>();
        for(int i = 0; i < splits.size(); i++) {
            splitChResults.add( splits.get(i).modify(unaryGetPEId()) );
        }
        
        TStream<String> splitChFanin = splitChResults.get(0).union(
                        new HashSet<>(splitChResults.subList(1, splitChResults.size())));
        
        /////////////////////////////////////
        TStream<String> all = splitChFanin.endLowLatency();

        Tester tester = topology.getTester();
        
        Condition<Long> uCount = tester.tupleCount(all, strs.length);
        
        Condition<List<String>> contents = tester.stringContents(all);
        Condition<List<String>> s2contents = tester.stringContents(s2);

        complete(tester, uCount, 10, TimeUnit.SECONDS);

        Set<String> peIds = new HashSet<>();
        peIds.addAll(contents.getResult());
        peIds.addAll(s2contents.getResult());
        

        assertEquals("peIds: "+peIds, 1, peIds.size() );
    }
    
    @SuppressWarnings("serial")
    static UnaryOperator<String> unaryGetPEId() {
        return new UnaryOperator<String>() {
            @Override
            public String apply(String v) {
                return PERuntime.getPE().getPEId().toString();
            }
        }; 
    }
    
    @SuppressWarnings("serial")
    private static ToIntFunction<String> roundRobinSplitter() {
        return new ToIntFunction<String>() {
            private int i;

            @Override
            public int applyAsInt(String s) {
                return i++;
            }
        };
    }
    
    @Test
    public void testNested() throws Exception {
        // Uses Condition.getResult - not supported.
        assumeTrue(!isStreamingAnalyticsRun());
        
        // ensure nested low latency yields all fns in the same container
        
        final Topology topology = newTopology("nestedTest");
        final Tester tester = topology.getTester();
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        String[] s1Strs = {"a"};
        TStream<String> s1 = topology.strings(s1Strs);

        TStream<String> s2 = 
                s1
                .isolate()
                .lowLatency()
                    .modify(getContainerIdAppend())
                    .lowLatency()
                        .modify(getContainerIdAppend())
                    .endLowLatency()
                    .modify(getContainerIdAppend())
                .endLowLatency()
                ;
        
        Condition<Long> uCount = tester.tupleCount(s2.filter(new AllowAll<String>()), 1);
        Condition<List<String>> contents = tester.stringContents(
                s2.filter(new AllowAll<String>()));

        complete(tester, uCount, 10, TimeUnit.SECONDS);

        Set<String> ids = getContainerIds(contents.getResult());
        assertEquals("ids: "+ids, 1, ids.size());
    }
    
    private static ThreadLocal<Long> sameThread = new ThreadLocal<>();

    /**
     * Test the same thread executes the low latency section.
     */
    @Test
    public void testSameThread() throws Exception {
        assumeTrue(SC_OK);
        final int tc = 2000;
        final Topology topology = newTopology("testSameThread");
        final Tester tester = topology.getTester();
        
        TStream<Long> s1 = topology.limitedSource(new Rnd(), tc);
        TStream<Long> s2 = topology.limitedSource(new Rnd(), tc);
        TStream<Long> s3 = topology.limitedSource(new Rnd(), tc);
        TStream<Long> s4 = topology.limitedSource(new Rnd(), tc);
        
        TStream<Long> s = s1.union(new HashSet<>(Arrays.asList(s2, s3, s4)));
        s = s.lowLatency();
        s = s.transform(new SetThread());
        for (int i = 0 ; i < 20; i++)
            s = s.transform(new CheckThread());
        
        s = s.transform(new ClearThread());
        s = s.endLowLatency();
        s = s.filter(t -> true);
        
        this.getConfig().put(com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS, Boolean.TRUE);
        
        Condition<Long> endCondition = tester.tupleCount(s, 4 * tc);
        
        this.complete(tester, endCondition, 30, TimeUnit.SECONDS);
        
    }
    
    @SuppressWarnings("serial")
    public static class SetThread implements UnaryOperator<Long> {
        @Override
        public Long apply(Long v) {
            sameThread.set(v);
            return v;
        }
    }
    @SuppressWarnings("serial")
    public static class CheckThread implements UnaryOperator<Long> {
        @Override
        public Long apply(Long v) {
            if (!v.equals(sameThread.get()))
                throw new IllegalStateException("Thread mismatch:" +
                   Thread.currentThread().getName() + " expected:" +
                        v + " thread local:" + sameThread.get());
            return v;
        }
    }
    @SuppressWarnings("serial")
    public static class ClearThread implements UnaryOperator<Long> {
        @Override
        public Long apply(Long v) {
            sameThread.set(null);
            return v;
        }
    }
      
    
    @SuppressWarnings("serial")
    public static class Rnd implements Supplier<Long> {
        
        private transient Random r;

        @Override
        public Long get() {
            if (r == null)
                r = new Random();
            return r.nextLong();
        }
        
    }
}
