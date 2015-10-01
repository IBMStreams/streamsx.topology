package com.ibm.streamsx.topology.test.api;

import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerId;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIdAppend;
import static com.ibm.streamsx.topology.test.api.IsolateTest.getContainerIds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class LowLatencyTest extends TestTopology {
    @Test
    public void simpleLowLatencyTest() throws Exception{
        assumeTrue(SC_OK);
        assumeTrue(isMainRun());
        Topology topology = new Topology("lowLatencyTest");

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
        Topology topology = new Topology("lowLatencyTest");

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
        Topology topology = new Topology("lowLatencyTest");

        // Construct topology
        TStream<String> ss = topology.strings("hello").lowLatency();
        TStream<String> ss1 = ss.transform(getContainerId());
        TStream<String> ss2 = ss1.transform(getContainerId()).endLowLatency();
        
        SPLGenerator generator = new SPLGenerator();
        JSONObject graph = topology.builder().complete();
        generator.generateSPL(graph);
        
        JSONArray ops = (JSONArray)graph.get("operators");
        for(Object opObj : ops){
            JSONObject op = (JSONObject)opObj;
            String lowLatencyTag = null;
            JSONObject placement = JOperatorConfig.getJSONItem(op, JOperatorConfig.PLACEMENT);
            if (placement != null)
                lowLatencyTag = (String) placement.get(JOperator.PLACEMENT_LOW_LATENCY_REGION_ID);
            String kind = (String)op.get("kind");
            JSONObject queue = (JSONObject) op.get("queue");
            if(queue != null && (lowLatencyTag!=null || lowLatencyTag.equals(""))){
                throw new IllegalStateException("Operator has threaded port when it shouldn't.");
            }
            if(queue != null 
                    && kind.equals("com.ibm.streamsx.topology.functional.java::FunctionTransform")){
                throw new IllegalStateException("Transform operator expecting threaded port; none found.");
            }
        }
    }
    
    @Test
    public void testLowLatencySplit() throws Exception {
        
        // lowLatency().split() is an interesting case because split()
        // has >1 oports.
        
        final Topology topology = new Topology("testLowLatencySplit");
        
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
        
        final Topology topology = new Topology("nestedTest");
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

}
