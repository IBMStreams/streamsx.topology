/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.FunctionContainer;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Initializable;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.streams.CollectionStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class StreamTest extends TestTopology {

    public static void assertStream(Topology f, TStream<?> stream) {
        TopologyTest.assertFlowElement(f, stream);
    }

    @Test
    public void testBasics() throws Exception {
        assumeTrue(isMainRun());
        final Topology topology = new Topology("BasicStream");
        
        assertEquals("BasicStream", topology.getName());
        assertSame(topology, topology.topology());
        
        TStream<String> source = topology.strings("a", "b", "c");
        assertStream(topology, source);
        assertSame(String.class, source.getTupleClass());
        assertSame(String.class, source.getTupleType());
        
        assertNotSame(source, source.autonomous());
    }

    @Test
    public void testStringFilter() throws Exception {
        final Topology f = newTopology("StringFilter");
        TStream<String> source = f.strings("hello", "goodbye", "farewell");
        assertStream(f, source);

        TStream<String> filtered = source.filter(lengthFilter(5));
        
        completeAndValidate(filtered, 10, "goodbye", "farewell");
    }

    @SuppressWarnings("serial")
    public static Predicate<String> lengthFilter(final int length) {
        return new Predicate<String>() {

            @Override
            public boolean test(String v1) {
                return v1.length() > length;
            }
        };
    }

    @Test
    public void testTransform() throws Exception {
        final Topology f = newTopology("TransformStream");
        TStream<String> source = f.strings("325", "457", "9325");
        assertStream(f, source);

        TStream<Integer> i1 = source.map(stringToInt());
        TStream<Integer> i2 = i1.transform(add17());
        completeAndValidate(i2, 10, "342", "474", "9342");
    }

    @Test
    public void testTransformWithDrop() throws Exception {
        final Topology f = newTopology("TransformStream");
        TStream<String> source = f.strings("93", "68", "221");
        assertStream(f, source);

        TStream<Integer> i1 = source.transform(stringToIntExcept68());
        TStream<Integer> i2 = i1.map(add17());
        
        completeAndValidate(i2, 10, "110", "238");
    }

    @Test
    public void testMultiTransform() throws Exception {
        final Topology topology = newTopology("MultiTransformStream");
        TStream<String> source = topology.strings("mary had a little lamb",
                "its fleece was white as snow");
        assertStream(topology, source);

        TStream<String> words = source.flatMap(splitWords());
        
        completeAndValidate(words, 10, "mary", "had",
                "a", "little", "lamb", "its", "fleece", "was", "white", "as",
                "snow");
    }

    @Test
    public void testUnionNops() throws Exception {
        assumeTrue(isMainRun());
        final Topology f = newTopology("Union");
        TStream<String> s1 = f.strings("A1", "B1", "C1", "D1");

        Set<TStream<String>> empty = Collections.emptySet();
        assertSame(s1, s1.union(s1));
        assertSame(s1, s1.union(empty));
        assertSame(s1, s1.union(Collections.singleton(s1)));
    }

    @Test
    public void testUnion() throws Exception {
        final Topology topology = newTopology("Union");
        TStream<String> s1 = topology.strings("A1", "B1", "C1", "D1");
        TStream<String> s2 = topology.strings("A2", "B2", "C2", "D2");
        List<String> l3 = new ArrayList<>();
        l3.add("A3");
        l3.add("B3");
        TStream<String> s3 = topology.constants(l3);
        
        List<String> l4 = new ArrayList<>();
        l4.add("A4");
        l4.add("B4");
        TStream<String> s4 = topology.constants(l4);

        assertNotSame(s1, s2);

        TStream<String> su = s1.union(s2);

        assertNotSame(su, s1);
        assertNotSame(su, s2);
        
        // Merge with two different schema types
        // but the primary has the correct direct type.
        su = su.union(s3);   
        assertEquals(String.class, su.getTupleClass());
        assertEquals(String.class, su.getTupleType());
        
        // Merge with two different schema types
        // but the primary has the generic type
        assertNull(s4.getTupleClass());
        su = s4.union(su);
        assertEquals(String.class, su.getTupleClass());
        assertEquals(String.class, su.getTupleType());
        
        su = su.filter(new AllowAll<String>());

        Tester tester = topology.getTester();

        Condition<Long> suCount = tester.tupleCount(su, 12);
        Condition<List<String>> suContents = tester.stringContentsUnordered(su, "A1", "B1", "C1", "D1", "A2", "B2", "C2", "D2", "A3", "B3", "A4", "B4");

        //assertTrue(complete(tester, suCount, 10, TimeUnit.SECONDS));
        complete(tester, suCount, 10, TimeUnit.SECONDS);

        assertTrue("SU Contents", suContents.valid());
        assertTrue("SU Count", suCount.valid());

    }

    @Test
    public void testUnionSet() throws Exception {
        final Topology topology = newTopology("Union");
        TStream<String> s1 = topology.strings("A1", "B1", "C1");
        TStream<String> s2 = topology.strings("A2", "B2", "C2", "D2");
        TStream<String> s3 = topology.strings("A3", "B3", "C3");
        Set<TStream<String>> streams = new HashSet<TStream<String>>();
        streams.add(s2);
        streams.add(s3);
        TStream<String> su = s1.union(streams);

        // TODO - testing doesn't work against union streams in embedded.
        su = su.filter(new AllowAll<String>());

        Tester tester = topology.getTester();

        Condition<Long> suCount = tester.tupleCount(su, 10);
        Condition<List<String>> suContents = tester.stringContentsUnordered(su, "A1", "B1",
                "C1", "A2", "B2", "C2", "D2", "A3", "B3", "C3");

        assertTrue(complete(tester, suCount, 10, TimeUnit.SECONDS));

        // assertTrue("SU:" + suContents, suContents.valid());
        assertTrue("SU:" + suCount, suCount.valid());
        assertTrue("SUContents:" + suContents, suContents.valid());
    }

    @Test
    public void testSimpleParallel() throws Exception {

        final Topology topology = newTopology("EmbeddedParallel");
        TStream<Number> s1 = topology.numbers(1, 2, 3, 94, 5, 6).parallel(6)
                .filter(new AllowAll<Number>()).endParallel();

        TStream<String> sp = StringStreams.toString(s1);

        Tester tester = topology.getTester();

        Condition<Long> spCount = tester.tupleCount(sp, 6);
        Condition<List<String>> spContents = tester.stringContentsUnordered(sp, "1", "2",
                "3", "94", "5", "6");

        complete(tester, spCount, 60, TimeUnit.SECONDS);

        // assertTrue("SU:" + suContents, suContents.valid());
        assertTrue("SP:" + spCount, spCount.valid());
        assertTrue("SPContents:" + spContents, spContents.valid());
    }
    
    @Test
    public void testSplit() throws Exception {
        final Topology topology = newTopology("testSplit");
        
        TStream<String> s1 = topology.strings("ch0", "ch1", "ch2", "omit",
                    "another-ch2", "another-ch1", "another-ch0", "another-omit");

        List<TStream<String>> splits = s1.split(3, myStringSplitter());

        assertEquals("list size", 3, splits.size());

        Tester tester = topology.getTester();
        
        List<Condition<List<String>>> contents = new ArrayList<>();
        for(int i = 0; i < splits.size(); i++) {
            TStream<String> ch = splits.get(i);
            Condition<List<String>> chContents = 
                    tester.stringContents(ch, "ch"+i, "another-ch"+i);
            contents.add(chContents);
        }

        TStream<String> all = splits.get(0).union(
                new HashSet<>(splits.subList(1, splits.size())));
        Condition<Long> uCount = tester.tupleCount(all, 6);

        complete(tester, uCount, 10, TimeUnit.SECONDS);

        for(int i = 0; i < splits.size(); i++) {
            assertTrue("chContents["+i+"]:" + contents.get(i), contents.get(i).valid());
        }
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
    

    @SuppressWarnings("serial")
    static Function<String, Integer> stringToInt() {
        return new Function<String, Integer>() {

            @Override
            public Integer apply(String v1) {
                return Integer.valueOf(v1);
            }
        };
    }

    @SuppressWarnings("serial")
    static Function<String, Integer> stringToIntExcept68() {
        return new Function<String, Integer>() {

            @Override
            public Integer apply(String v1) {
                Integer i = Integer.valueOf(v1);
                return (i == 68) ? null : i;
            }
        };
    }

    @SuppressWarnings("serial")
    static Function<Integer, Integer> add17() {
        return new Function<Integer, Integer>() {

            @Override
            public Integer apply(Integer v1) {
                return v1 + 17;
            }
        };
    }

    @SuppressWarnings("serial")
    static Function<String, Iterable<String>> splitWords() {
        return new Function<String, Iterable<String>>() {

            @Override
            public Iterable<String> apply(String v1) {
                return Arrays.asList(v1.split(" "));
            }
        };
    }
    
    
    @Test
    public void testFunctionContextNonDistributed() throws Exception {
        
        assumeTrue(getTesterType() == Type.STANDALONE_TESTER || getTesterType() == Type.EMBEDDED_TESTER);
        
        Topology t = newTopology();
        TStream<Map<String, Object>> values = BeaconStreams.single(t).transform(new ExtractFunctionContext());
        TStream<String> strings = StringStreams.toString(CollectionStreams.flattenMap(values));
                
        Tester tester = t.getTester();
        
        Condition<Long> spCount = tester.tupleCount(strings, 9);
        Condition<List<String>> spContents = tester.stringContents(strings, 
                "channel=-1",
                "domainId=" + System.getProperty("user.name"),
                "id=0",
                "instanceId=" + System.getProperty("user.name"),
                "jobId=0",
                "jobName=NOTNULL",
                "maxChannels=0",
                "noAppConfig={}",
                "relaunchCount=0"
                );

        complete(tester, spCount, 60, TimeUnit.SECONDS);

        // assertTrue("SU:" + suContents, suContents.valid());
        assertTrue("SP:" + spCount, spCount.valid());
        assertTrue("SPContents:" + spContents, spContents.valid());
    }
    
    public static class ExtractFunctionContext
         implements Function<Long,Map<String,Object>>, Initializable {
        private static final long serialVersionUID = 1L;
        private transient FunctionContext functionContext;
        
        @Override
        public Map<String, Object> apply(Long v) {
            Map<String,Object> values = new TreeMap<>();
            
            values.put("channel", functionContext.getChannel());
            values.put("maxChannels", functionContext.getMaxChannels());
            
            FunctionContainer container = functionContext.getContainer();
            values.put("id", container.getId());
            values.put("jobId", container.getJobId());
            values.put("relaunchCount", container.getRelaunchCount());
            
            values.put("domainId", container.getDomainId());
            values.put("instanceId", container.getInstanceId());
            
            values.put("jobName", container.getJobName() == null ? "NULL" : "NOTNULL");
            values.put("noAppConfig", container.getApplicationConfiguration("no_such_config"));
            
            return values;
        }
        
        @Override
        public void initialize(FunctionContext functionContext)
                throws Exception {
            this.functionContext = functionContext;
            
        }
        
    }
    
    /**
     * Ensure we can create the three types of metrics.
     * @throws Exception
     */
    @Test
    public void testMetricCreate() throws Exception {

        final Topology topo = new Topology();
        
        TStream<String> strings = topo.strings("a", "b", "c");
        Tester tester = topo.getTester();
        
        Condition<Long> spCount = tester.tupleCount(strings, 3);
        complete(tester, spCount, 20, TimeUnit.SECONDS);
    }
    
    public static class CreateMetricTester<T> implements UnaryOperator<T>, Initializable {
        private static final long serialVersionUID = 1L;

        @Override
        public T apply(T v) {
            return v;
        }

        @Override
        public void initialize(FunctionContext functionContext) throws Exception {
            functionContext.createCustomMetric("aCounter", "Counter desc.",
                    "counter", () -> this.hashCode());
            
            functionContext.createCustomMetric("aTimer", "the time!", "time",
                    System::currentTimeMillis); 
            
            Random r = new Random();
            functionContext.createCustomMetric("aGauge", "Some gauge", "gauge",
                    r::nextLong);
        }
        
    }
}
