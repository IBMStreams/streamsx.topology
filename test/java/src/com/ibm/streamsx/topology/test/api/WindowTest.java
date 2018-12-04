/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class WindowTest extends TestTopology {

    public static final class PeriodicStrings implements Supplier<String> {
        private static final long serialVersionUID = 1L;
        private int id;

        @Override
        public String get() {
            return Integer.toString(id++) + ":" + Long.toString(System.currentTimeMillis()); 
        }
    }

    @SuppressWarnings("serial")
    private static final class SumInt implements
            Function<List<Number>, Integer> {
        @Override
        public Integer apply(List<Number> v1) {
            int sum = 0;
            int count = 0;
            for (Number i : v1) {
                count++;
                sum += i.intValue();
            }
            if (count > 3)
                throw new IllegalStateException("more than three tuples for last(3)");
            return sum;
        }
    }

    public static void assertWindow(Topology f, TWindow<?,?> window) {
        TopologyTest.assertFlowElement(f, window);
    }

    @Test
    public void testBasicCount() throws Exception {
        final Topology f = newTopology("CountWindow");
        TStream<String> source = f.strings("a", "b", "c");
        TWindow<String,?> window = source.last(10);
        assertNotNull(window);
        assertWindow(f, window);
    }

    @Test
    public void testBasicTime() throws Exception {
        final Topology f = newTopology("TimeWindow");
        TStream<String> source = f.strings("a", "b", "c");
        TWindow<String,?> window = source.last(10, TimeUnit.SECONDS);
        assertNotNull(window);
        assertWindow(f, window);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testZeroTimeWindow() throws Exception {
        final Topology f = newTopology("ZeroTimeWindow");
        TStream<String> source = f.strings("a", "b", "c");
        source.last(0, TimeUnit.DAYS);
    } 
    
    @Test(expected=IllegalArgumentException.class)
    public void testZeroTimeAggregate() throws Exception {
        final Topology f = newTopology("ZeroTimeWindow");
        TStream<Number> source = f.numbers(1, 2, 3, 4, 5, 6, 7);
        source.last(1, TimeUnit.DAYS).aggregate(new SumInt(), 0, TimeUnit.HOURS);
    }
    

    @Test
    public void testCountAggregate() throws Exception {
        final Topology f = newTopology("CountAggregate");
        TStream<Number> source = f.numbers(1, 2, 3, 4, 5, 6, 7);
        TWindow<Number,?> window = source.last(3);
        TStream<Integer> aggregate = window.aggregate(new SumInt());
        
        completeAndValidate(aggregate, 10, "1", "3", "6", "9", "12", "15", "18");
    }

    @Test
    public void testKeyedAggregate() throws Exception {
        TStream<StockPrice> aggregate = _testKeyedAggregate();
        
        completeAndValidate(aggregate, 10, "A:1000", "B:4004", "C:2013", "A:1005",
                "A:1010", "B:4005", "A:1010", "C:2007", "B:4008", "C:2003",
                "A:1015", "B:4010", "B:4009", "B:4008", "A:1021", "C:2005",
                "C:2018", "A:1024");
    }
    
    private static TStream<StockPrice> _testKeyedAggregate() throws Exception {

        final Topology f = newTopology("PartitionedAggregate");
        TStream<StockPrice> source = f.constants(Arrays.asList(PRICES)).asType(StockPrice.class);

        TStream<StockPrice> aggregate = source.last(2).key(new Function<StockPrice,String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String apply(StockPrice v) {
                return v.getKey();
            }}).aggregate(new AveragePrice());
        
        return aggregate;
    }

    static final StockPrice[] PRICES = { new StockPrice("A", 1000),
            new StockPrice("B", 4004), new StockPrice("C", 2013),
            new StockPrice("A", 1010), new StockPrice("A", 1011),
            new StockPrice("B", 4007), new StockPrice("A", 1010),
            new StockPrice("C", 2002), new StockPrice("B", 4010),
            new StockPrice("C", 2004), new StockPrice("A", 1020),
            new StockPrice("B", 4010), new StockPrice("B", 4009),
            new StockPrice("B", 4008), new StockPrice("A", 1022),
            new StockPrice("C", 2007), new StockPrice("C", 2030),
            new StockPrice("A", 1026),

    };
    
    // Aggregate from a keyed stream.
    @Test
    public void testKeyedStreamAggregate() throws Exception {
        TStream<StockPrice> aggregate = _testStreamKeyedAggregate();
        
        completeAndValidate(aggregate, 10, "A:1000", "B:4004", "C:2013", "A:1005",
                "A:1010", "B:4005", "A:1010", "C:2007", "B:4008", "C:2003",
                "A:1015", "B:4010", "B:4009", "B:4008", "A:1021", "C:2005",
                "C:2018", "A:1024");
    }
    
    private static TStream<StockPrice> _testStreamKeyedAggregate() throws Exception {

        final Topology f = newTopology("KeyedStreamAggregate");

        TStream<StockPrice> source = f.constants(Arrays.asList(PRICES)).asType(StockPrice.class);        

        TStream<StockPrice> aggregate = source.last(2).key(StockPrice::getKey).aggregate(new AveragePrice());
        
        return aggregate;
    }

    public static class StockPrice implements Serializable {

        private static final long serialVersionUID = 1L;
        
        private final String ticker;
        private final int price;

        public StockPrice(String ticker, int price) {
            this.ticker = ticker;
            this.price = price;
        }

        public String getKey() {
            return ticker;
        }

        public int getPrice() {
            return price;
        }

        public String toString() {
            return getKey() + ":" + getPrice();
        }

    }

    @SuppressWarnings("serial")
    public static class AveragePrice implements
            Function<List<StockPrice>, StockPrice> {

        @Override
        public StockPrice apply(List<StockPrice> tuples) {
            int price = 0;
            int count = 0;
            StockPrice last = null;
            for (StockPrice tuple : tuples) {
                count++;
                price += tuple.getPrice();
                last = tuple;
            }

            if (count == 0)
                return null;

            return new StockPrice(last.getKey(), price / count);
        }

    }
    
    /**
     * Test a continuous aggregation.
     */
    @Test
    public void testContinuousAggregateLastSeconds() throws Exception {
        // Uses JSON4J
        assumeTrue(hasStreamsInstall());
        
        final Topology t = newTopology();
        TStream<String> source = t.periodicSource(new PeriodicStrings(), 100, TimeUnit.MILLISECONDS);
        
        TStream<JSONObject> aggregate = source.last(3, TimeUnit.SECONDS).aggregate(new AggregateStrings());
        TStream<String> strings = JSONStreams.serialize(aggregate);
        
        Tester tester = t.getTester();
                
        Condition<String> checker = tester.stringTupleTester(strings, new ContinuousAggregateTester());
        
        // 10 tuples per second, each is aggregated, so 15 seconds is around 150 tuples.
        Condition<Long> ending = tester.atLeastTupleCount(strings, 150);
        complete(tester, ending, 30, TimeUnit.SECONDS);
        
        assertTrue(ending.valid()); 
        assertTrue(checker.valid()); 
    }
    
    public static class ContinuousAggregateTester implements Predicate<String> {
        private static final long serialVersionUID = 1L;
        private long startTs;
        
        @Override
        public boolean test(String output) {
            JSONObject agg;
            try {
                agg = JSONObject.parse(output);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            JSONArray items  = (JSONArray) agg.get("items");
            long ts = (Long) agg.get("ts");
            
            // Should see around 30 tuples per window, once we
            // pass the first three seconds.
            if (items.size() > 45) {
                System.err.println("Number of tuples in window > 45:" + items.size());
                return false;
            }
            if (agg.containsKey("delta")) {
                long delta = (Long) agg.get("delta");
                if (delta < 0) {
                    System.err.println("Negative delta:" + delta);
                    return false;
                }
                                   
                if (startTs == 0) {
                    startTs = ts;
                } else {
                    long diff = ts - startTs;
                    if (diff > 3000) {
                        if (items.size() < 25) {
                            System.err.println("Number of tuples in window < 25:" + items.size());
                            return false;
                        }
                    }
                }
            }
            return true;
        }
        
    }
    
    
    
    
    /**
     * Test a periodic aggregation.
     */
    @Test
    public void testPeriodicAggregateLastSeconds() throws Exception {
        
        final Topology t = newTopology();
        TStream<String> source = t.periodicSource(new PeriodicStrings(), 100, TimeUnit.MILLISECONDS);
        
        TStream<JSONObject> aggregate = source.last(3, TimeUnit.SECONDS).aggregate(
                new AggregateStrings(), 1, TimeUnit.SECONDS);
        TStream<String> strings = JSONStreams.serialize(aggregate);
        
        Tester tester = t.getTester();
        
        Condition<String> checker = tester.stringTupleTester(strings, new PeriodicAggregateTester());
        
        // 10 tuples per second, aggregate every second, so 15 seconds is around 15 tuples.
        Condition<Long> ending = tester.atLeastTupleCount(strings, 15);
        complete(tester, ending, 30, TimeUnit.SECONDS);
        
        assertTrue(ending.valid()); 
        assertTrue(checker.valid()); 
    }
    
    public static class PeriodicAggregateTester implements Predicate<String> {
        private static final long serialVersionUID = 1L;
        
        private long startTs = 0;

        @Override
        public boolean test(String output) {
            JSONObject agg;
            try {
                agg = JSONObject.parse(output);
            } catch (IOException e) {
                e.printStackTrace(System.err);
                return false;
            }
            JSONArray items  = (JSONArray) agg.get("items");
            long ts = (Long) agg.get("ts");
            
            // Should see around 30 tuples per window, once we
            // pass the first three seconds.
            if (items.size() > 45) {
                System.err.println("Number of tuples in window > 45:" + items.size());
                return false;
            }
            if (agg.containsKey("delta")) {
                long delta = (Long) agg.get("delta");
                if (delta < 0) {
                    System.err.println("Negative delta:" + delta);
                    return false;
                }
                if (!(delta > 800 && delta < 1200)) {
                    System.err.println("timeBetweenAggs: " + delta);
                    return false;                    
                }

                if (startTs == 0) {
                    startTs = ts;
                } else {
                    long diff = ts - startTs;
                    if (diff > 3000) {
                        if (items.size() < 25) {
                            System.err.println("Number of tuples in window < 25:" + items.size());
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }
    
    /**
     * Test a periodic aggregation with microsecond aggregation.
     * Basically a test that the application runs, hard to test
     * for specific results for such a short window.
     */
    @Test
    @Ignore("Java SPL not supporting small window sizes")
    public void testPeriodicAggregateLastMicroseconds() throws Exception {
        
        // Embedded doesn't support window sizes < 1ms (see issue #211)
        assumeTrue(!isEmbedded());        
        
        final Topology t = newTopology();
        TStream<String> source = t.periodicSource(new PeriodicStrings(), 10, TimeUnit.MILLISECONDS);
        
        TStream<JSONObject> aggregate = source.last(3, TimeUnit.MICROSECONDS).aggregate(
                new AggregateStrings(), 10, TimeUnit.MICROSECONDS);
        TStream<String> strings = JSONStreams.serialize(aggregate);
        
        Tester tester = t.getTester();
                
        // Aggregate 10 microseconds, so 2 seconds is around 200,000 tuples.
        Condition<Long> ending = tester.atLeastTupleCount(strings, 200_000);
        complete(tester, ending, 30, TimeUnit.SECONDS);
    }
    
    
    public static class AggregateStrings implements Function<List<String>, JSONObject> {
        private static final long serialVersionUID = 1L;
        
        private transient long lastts;
        private transient int count;

        @Override
        public JSONObject apply(List<String> v) {
            JSONObject agg = new JSONObject();
            JSONArray items = new JSONArray();
            for (String e : v)
                items.add(e);
            agg.put("items", items);
            long ts = System.currentTimeMillis();
            agg.put("ts", ts);
            if (lastts != 0)
                agg.put("delta", ts - lastts);
            
            lastts = ts;
            agg.put("count", ++count);


            return agg;
        }
    }
}
