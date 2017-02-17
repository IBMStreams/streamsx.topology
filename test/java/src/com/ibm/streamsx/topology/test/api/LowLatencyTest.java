/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.CONFIG;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_LOW_LATENCY_REGION_ID;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerId;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIdAppend;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIds;
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

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class LowLatencyTest extends TestTopology {
    @Test
    public void simpleLowLatencyTest() throws Exception{
        assumeTrue(SC_OK);
        assumeTrue(isMainRun());
        Topology topology = newTopology("lowLatencyTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello");
        TStream<String> ss1 = ss.transform(getContainerId()).lowLatency();
        TStream<String> ss2 = ss1.transform(getContainerId()).endLowLatency();
        ss2.print();
        
        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT).submit(topology).get();
    }
    
    @Test
    public void multipleRegionLowLatencyTest() throws Exception{
        assumeTrue(SC_OK);
        assumeTrue(isMainRun());
        Topology topology = newTopology("lowLatencyTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello")
                .transform(getContainerId()).transform(getContainerId());
        
        TStream<String> ss1 = ss.transform(getContainerId()).lowLatency();
        TStream<String> ss2 = ss1.transform(getContainerId()).
                transform(getContainerId()).endLowLatency().transform(getContainerId());
        TStream<String> ss3 = ss2.transform(getContainerId()).lowLatency();
        ss3.transform(getContainerId()).transform(getContainerId())
            .endLowLatency().print();
        
        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT).submit(topology).get();
    }
    
    @Test
    public void threadedPortTest() throws Exception{
        assumeTrue(isMainRun());
        Topology topology = newTopology("lowLatencyTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello").lowLatency();
        TStream<String> ss1 = ss.transform(getContainerId());
        TStream<String> ss2 = ss1.transform(getContainerId()).endLowLatency();
        
        SPLGenerator generator = new SPLGenerator();
        JSONObject graph = topology.builder().complete();
        
        JsonObject ggraph = JSON4JUtilities.gson(graph);
        generator.generateSPL(ggraph);
        
        GsonUtilities.objectArray(ggraph , "operators", op -> {
            String lowLatencyTag = null;
            JsonObject placement = object(op, CONFIG, PLACEMENT);
            if (placement != null)
                lowLatencyTag = jstring(placement, PLACEMENT_LOW_LATENCY_REGION_ID);
            String kind = jstring(op, "kind");
            JsonObject queue = object(op, "queue");
            if(queue != null && (lowLatencyTag == null || lowLatencyTag.equals(""))){
                throw new IllegalStateException("Operator has threaded port when it shouldn't.");
            }
            if(queue != null 
                    && kind.equals("com.ibm.streamsx.topology.functional.java::FunctionTransform")){
                throw new IllegalStateException("Transform operator expecting threaded port; none found.");
            }
        });
    }
    
    @Test
    public void testLowLatencySplit() throws Exception {
        
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
        TStream<String> s2 = s1.modify(unaryGetPEId());
        
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
        
        Condition<List<String>> contents = tester.stringContents(all, "");
        Condition<List<String>> s2contents = tester.stringContents(s2, "");

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
    public void nestedTest() throws Exception {
        
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
        
        // NOTE, this works in the sense that all end up in the same container,
        // but currently only because of the default fuse-island behavior.
        // There are two issues with the json:
        // a) the 3rd modify is missing a lowLatencyTag
        // b) the 2nd modify has a different tag than the first.
        //    logically it must net out to being in the same container,
        //    so its just easiest if they're the same tag.
        //    It's not clear that having them be different is an absolute wrong,
        //    it's just that it doesn't add any value and complicates things.
        
        // s2.print();
        
        Condition<Long> uCount = tester.tupleCount(s2.filter(new AllowAll<String>()), 1);
        Condition<List<String>> contents = tester.stringContents(
                s2.filter(new AllowAll<String>()), "");

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
