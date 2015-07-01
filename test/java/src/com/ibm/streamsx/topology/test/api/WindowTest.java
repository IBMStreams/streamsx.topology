/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tuple.Keyable;

public class WindowTest extends TestTopology {

    @SuppressWarnings("serial")
    private static final class SumInt implements
            Function<Iterable<Number>, Integer> {
        @Override
        public Integer apply(Iterable<Number> v1) {
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

    public static void assertWindow(Topology f, TWindow<?> window) {
        TopologyTest.assertFlowElement(f, window);
    }

    @Test
    public void testBasicCount() throws Exception {
        final Topology f = new Topology("CountWindow");
        TStream<String> source = f.strings("a", "b", "c");
        TWindow<String> window = source.last(10);
        assertNotNull(window);
        assertWindow(f, window);
    }

    @Test
    public void testBasicTime() throws Exception {
        final Topology f = new Topology("TimeWindow");
        TStream<String> source = f.strings("a", "b", "c");
        TWindow<String> window = source.last(10, TimeUnit.SECONDS);
        assertNotNull(window);
        assertWindow(f, window);
    }

    @Test
    public void testCountAggregate() throws Exception {
        final Topology f = new Topology("CountAggregate");
        TStream<Number> source = f.numbers(1, 2, 3, 4, 5, 6, 7);
        TWindow<Number> window = source.last(3);
        TStream<Integer> aggregate = window.aggregate(new SumInt(),
                Integer.class);
        
        completeAndValidate(aggregate, 10, "1", "3", "6", "9", "12", "15", "18");
    }

    @Test
    public void testKeyedAggregate() throws Exception {

        final Topology f = new Topology("PartitionedAggregate");
        TStream<StockPrice> source = f.constants(Arrays.asList(PRICES),
                StockPrice.class);

        TStream<StockPrice> aggregate = source.last(2).aggregate(new AveragePrice(), StockPrice.class);

        completeAndValidate(aggregate, 10, "A:1000", "B:4004", "C:2013", "A:1005",
                "A:1010", "B:4005", "A:1010", "C:2007", "B:4008", "C:2003",
                "A:1015", "B:4010", "B:4009", "B:4008", "A:1021", "C:2005",
                "C:2018", "A:1024");
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

    @SuppressWarnings("serial")
    public static class StockPrice implements Keyable<String> {

        private final String ticker;
        private final int price;

        public StockPrice(String ticker, int price) {
            this.ticker = ticker;
            this.price = price;
        }

        @Override
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
            Function<Iterable<StockPrice>, StockPrice> {

        @Override
        public StockPrice apply(Iterable<StockPrice> tuples) {
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

}
