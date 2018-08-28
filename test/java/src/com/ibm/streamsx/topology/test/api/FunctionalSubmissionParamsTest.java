/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * Test use of submission parameters in functional logic.
 * <p>
 * <p>
 * Really need to test in all 3 of DISTRIBUTED, STANDALONE, EMBEDDED.
 * <p>
 * See {@code ParallelTest} for submission parameter as a width specification
 * and submission parameter use within parallel regions (composites) as
 * SPL operator parameters and in functional logic in the region.
 * <p>
 * See {@code SPLOperatorsTest} for general submission parameter testing
 * for SPL operator parameters.
 */
public class FunctionalSubmissionParamsTest extends TestTopology {

    @Test
    public void testFilterWithSubmissionParams() throws Exception {
        Topology topology = newTopology();
        
        Supplier<Integer> threshold = topology.createSubmissionParameter("threshold", Integer.class);
        Supplier<Integer> defaultedThreshold = topology.createSubmissionParameter("defaultedThreshold", 2);
        
        List<Integer> data = Arrays.asList(new Integer[] {1,2,3,4,5});
        
        TStream<Integer> s = topology.constants(data);
        TStream<Integer> defaultFiltered = s.filter(thresholdFilter(defaultedThreshold));
        TStream<Integer> filtered = s.filter(thresholdFilter(threshold));

        Map<String,Object> params = new HashMap<>();
        params.put("threshold", 3);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
        
        ////////////////////
        
        TStream<Integer> union = filtered.union(defaultFiltered);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount1 = tester.tupleCount(filtered, 2);
        Condition<Long> expectedCount2 = tester.tupleCount(defaultFiltered, 3);
        Condition<Long> expectedCount3 = tester.tupleCount(union, 5);
        
        complete(tester, expectedCount3, 15, TimeUnit.SECONDS);

        assertTrue(expectedCount1.toString(), expectedCount1.valid());
        assertTrue(expectedCount2.toString(), expectedCount2.valid());
    }

    @Test
    public void testSourceWithSubmissionParams() throws Exception {
        Topology topology = newTopology();
        
        Supplier<Integer> someInt = topology.createSubmissionParameter("someInt", Integer.class);
        Supplier<Integer> someIntD = topology.createSubmissionParameter("someIntD", 20);

        // The test's functional logic asserts it receives the expected SP value.
        
        TStream<Integer> s = topology.source(sourceIterableSupplier(someInt, 10));
        TStream<Integer> sD = topology.source(sourceIterableSupplier(someIntD, 20));

        Map<String,Object> params = new HashMap<>();
        params.put("someInt", 10);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
        
        ////////////////////

        TStream<Integer> union = s.union(sD);
               
        Tester tester = topology.getTester();
        Condition<Long> expectedCount1 = tester.tupleCount(s, 5);
        Condition<Long> expectedCount2 = tester.tupleCount(sD, 5);
        Condition<Long> expectedCount3 = tester.tupleCount(union, 10);
        
        complete(tester, expectedCount3, 15, TimeUnit.SECONDS);

        assertTrue(expectedCount1.toString(), expectedCount1.valid());
        assertTrue(expectedCount2.toString(), expectedCount2.valid());
    }

    @Test
    public void testPeriodicSourceWithSubmissionParams() throws Exception {
        Topology topology = newTopology();
        
        Supplier<Integer> someInt = topology.createSubmissionParameter("someInt", Integer.class);
        Supplier<Integer> someIntD = topology.createSubmissionParameter("someIntD", 20);
        
        // The test's functional logic asserts it receives the expected SP value.

        // HEADS UP.  issue#213 - exceptions in the supplier get silently eaten
        // and the stream just ends up with 0 tuples.
        
        TStream<Integer> s = topology.periodicSource(sourceSupplier(someInt, 10), 100, TimeUnit.MILLISECONDS);
        TStream<Integer> sD = topology.periodicSource(sourceSupplier(someIntD, 20), 100, TimeUnit.MILLISECONDS);

        TStream<Integer> ms = topology.periodicMultiSource(sourceIterableSupplier(someInt, 10), 100, TimeUnit.MILLISECONDS);
        TStream<Integer> msD = topology.periodicMultiSource(sourceIterableSupplier(someIntD, 20), 100, TimeUnit.MILLISECONDS);

        Map<String,Object> params = new HashMap<>();
        params.put("someInt", 10);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
        
        ////////////////////
        
        Set<TStream<Integer>> all = new HashSet<>( Arrays.asList(
                s, sD,
                ms, msD
                ));
        TStream<Integer> union = s.union(all);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount1 = tester.tupleCount(s, 5);
        Condition<Long> expectedCount2 = tester.tupleCount(sD, 5);
        Condition<Long> expectedCount3 = tester.tupleCount(ms, 5);
        Condition<Long> expectedCount4 = tester.tupleCount(msD, 5);
        Condition<Long> expectedCount = tester.tupleCount(union, 20);
        
        complete(tester, expectedCount, 15, TimeUnit.SECONDS);

        assertTrue(expectedCount1.toString(), expectedCount1.valid());
        assertTrue(expectedCount2.toString(), expectedCount2.valid());
        assertTrue(expectedCount3.toString(), expectedCount3.valid());
        assertTrue(expectedCount4.toString(), expectedCount4.valid());
    }
    

    @Test
    //@Ignore("Suddenly started failing on jenkins streamsx.topology - but only there (expected 100 got 0). Get the build working again.")
    public void testFunctionsWithSubmissionParams() throws Exception {
        Topology topology = newTopology();
        
        // FunctionFilter op is based on FunctionFunctor and the
        // latter is what knows about submission params.
        // Hence, given the implementation, this test should cover
        // all sub classes (modify,transform,split,sink,window,aggregate...).
        //
        // That really accounts for all functional
        // operators except FunctionSource and FunctionPeriodicSource and
        // we have tests for those.
        //
        // But we really should verify anyway...
        
        Supplier<Integer> someInt = topology.createSubmissionParameter("someInt", Integer.class);
        Supplier<Integer> someIntD = topology.createSubmissionParameter("someIntD", 2);
        
        List<Integer> data = Arrays.asList(new Integer[] {1,2,3,4,5});
        
        TStream<Integer> s = topology.constants(data);

        // The test's functional logic asserts it receives the expected SP value.
        // Its the main form of validation for the test.
        
        // TStream.modify
        TStream<Integer> modified = s.modify(unaryFn(someInt, 1));
        TStream<Integer> modifiedD = s.modify(unaryFn(someIntD, 2));
        
        // TStream.transform
        TStream<Integer> xformed = s.transform(functionFn(someInt, 1));
        TStream<Integer> xformedD = s.transform(functionFn(someIntD, 2));
        
        // TStream.multiTransform
        TStream<Integer> multiXformed = s.flatMap(functionIterableFn(someInt, 1));
        TStream<Integer> multiXformedD = s.multiTransform(functionIterableFn(someIntD, 2));

        // TStream.join
        TStream<Integer> joined = s.join(s.last(1), biFunctionListFn(someInt, 1));
        TStream<Integer> joinedD = s.join(s.last(1), biFunctionListFn(someIntD, 2));
        TStream<Integer> lastJoined = s.joinLast(s, biFunctionFn(someInt, 1));
        TStream<Integer> lastJoinedD = s.joinLast(s, biFunctionFn(someIntD, 2));
        
        // TStream.sink
        s.sink(sinkerFn(someInt, 1));
        s.forEach(sinkerFn(someIntD, 2));

        // TStream.split
        List<TStream<Integer>> split = s.split(2,toIntFn(someInt, 1));
        TStream<Integer> unionedSplit = split.get(0).union(split.get(1));
        List<TStream<Integer>> splitD = s.split(2,toIntFn(someIntD, 2));
        TStream<Integer> unionedSplitD = splitD.get(0).union(splitD.get(1));
        
        // TStream.window
        TWindow<Integer,?> win = s.window(s.last(1).key(functionFn(someInt, 1)));
        TStream<Integer> winAgg = win.aggregate(functionListFn(someIntD, 2));
        TWindow<Integer,?> winD = s.window(s.last(1).key(functionFn(someInt, 1)));
        TStream<Integer> winAggD = winD.aggregate(functionListFn(someIntD, 2));
        
        // TWindow.aggregate
        TStream<Integer> agg = s.last(1).aggregate(functionListFn(someInt, 1));
        TStream<Integer> aggD = s.last(1).aggregate(functionListFn(someIntD, 2));
        s.last(1).aggregate(functionListFn(someInt, 1), 1, TimeUnit.MILLISECONDS);
        s.last(1).aggregate(functionListFn(someIntD, 2), 1, TimeUnit.MILLISECONDS);

        // TWindow.key
        TStream<Integer> kagg = s.last(1).key(functionFn(someInt, 1)).aggregate(functionListFn(someIntD, 2));
        TStream<Integer> kaggD = s.last(1).key(functionFn(someInt, 1)).aggregate(functionListFn(someIntD, 2));

        Map<String,Object> params = new HashMap<>();
        params.put("someInt", 1);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
        
        ////////////////////
        
        Set<TStream<Integer>> all = new HashSet<>( Arrays.asList(
                modified, modifiedD,
                xformed, xformedD,
                multiXformed, multiXformedD,
                joined, joinedD,
                lastJoined, lastJoinedD,
                unionedSplit, unionedSplitD,
                winAgg, winAggD,
                agg, aggD,
                kagg, kaggD
                ));
        TStream<Integer> union = modified.union(all);
        // tester sees 0 tuples when they are really there so...
        union = union.filter(new AllowAll<Integer>());
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(union, all.size() * 5);
        
        complete(tester, expectedCount, 15, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
    }


    @Test
    public void testSubmissionParamsTypes() throws Exception {
        Topology topology = newTopology();
        
        // verify functional with all SP types
        
        Supplier<String> strSp = topology.createSubmissionParameter("strSp", String.class);
        Supplier<String> strSpD = topology.createSubmissionParameter("strSpD", "dude");
        Supplier<Byte> byteSp = topology.createSubmissionParameter("byteSp", Byte.class);
        Supplier<Byte> byteSpD = topology.createSubmissionParameter("byteSpD", (byte) 2);
        Supplier<Short> shortSp = topology.createSubmissionParameter("shortSp", Short.class);
        Supplier<Short> shortSpD = topology.createSubmissionParameter("shortSpD", (short) 4);
        Supplier<Integer> intSp = topology.createSubmissionParameter("intSp", Integer.class);
        Supplier<Integer> intSpD = topology.createSubmissionParameter("intSpD", 6);
        Supplier<Long> longSp = topology.createSubmissionParameter("longSp", Long.class);
        Supplier<Long> longSpD = topology.createSubmissionParameter("longSpD", (long) 8);
        Supplier<Float> floatSp = topology.createSubmissionParameter("floatSp", Float.class);
        Supplier<Float> floatSpD = topology.createSubmissionParameter("floatSpD", 10.0f);
        Supplier<Double> doubleSp = topology.createSubmissionParameter("doubleSp", Double.class);
        Supplier<Double> doubleSpD = topology.createSubmissionParameter("doubleSpD", 12.0d);
        
        List<Integer> data = Arrays.asList(new Integer[] {1,2,3,4,5});
        int expectedCnt = 5;
        
        TStream<Integer> s = topology.constants(data);

        // The test's functional logic asserts it receives the expected SP value.

        TStream<Integer> strSpFiltered = s.filter(predicateFn(strSp, "yo"));
        TStream<Integer> strSpDFiltered = s.filter(predicateFn(strSpD, "dude"));
        TStream<Integer> byteSpFiltered = s.filter(predicateFn(byteSp, (byte)1));
        TStream<Integer> byteSpDFiltered = s.filter(predicateFn(byteSpD, (byte)2));
        TStream<Integer> shortSpFiltered = s.filter(predicateFn(shortSp, (short)3));
        TStream<Integer> shortSpDFiltered = s.filter(predicateFn(shortSpD, (short)4));
        TStream<Integer> intSpFiltered = s.filter(predicateFn(intSp, 5));
        TStream<Integer> intSpDFiltered = s.filter(predicateFn(intSpD, 6));
        TStream<Integer> longSpFiltered = s.filter(predicateFn(longSp, (long)7));
        TStream<Integer> longSpDFiltered = s.filter(predicateFn(longSpD, (long)8));
        TStream<Integer> floatSpFiltered = s.filter(predicateFn(floatSp, 9.0f));
        TStream<Integer> floatSpDFiltered = s.filter(predicateFn(floatSpD, 10.0f));
        TStream<Integer> doubleSpFiltered = s.filter(predicateFn(doubleSp, 11.0d));
        TStream<Integer> doubleSpDFiltered = s.filter(predicateFn(doubleSpD, 12.0d));

        Map<String,Object> params = new HashMap<>();
        params.put("strSp", "yo");
        params.put("byteSp", (byte) 1);
        params.put("shortSp", (short) 3);
        params.put("intSp", 5);
        params.put("longSp", (long) 7);
        params.put("floatSp", 9.0f);
        params.put("doubleSp", 11.0d);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
        
        ////////////////////

        Set<TStream<Integer>> all = new HashSet<>( Arrays.asList(
                strSpFiltered, strSpDFiltered,
                byteSpFiltered, byteSpDFiltered,
                shortSpFiltered, shortSpDFiltered,
                intSpFiltered, intSpDFiltered,
                longSpFiltered, longSpDFiltered,
                floatSpFiltered, floatSpDFiltered,
                doubleSpFiltered, doubleSpDFiltered
                ));
        TStream<Integer> union = strSpFiltered.union(all);        
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount0 = tester.tupleCount(union, all.size() * expectedCnt);
        Condition<Long> expectedCount1 = tester.tupleCount(strSpFiltered, expectedCnt);
        Condition<Long> expectedCount2 = tester.tupleCount(strSpDFiltered, expectedCnt);
        Condition<Long> expectedCount3 = tester.tupleCount(byteSpFiltered, expectedCnt);
        Condition<Long> expectedCount4 = tester.tupleCount(byteSpDFiltered, expectedCnt);
        Condition<Long> expectedCount5 = tester.tupleCount(shortSpFiltered, expectedCnt);
        Condition<Long> expectedCount6 = tester.tupleCount(shortSpDFiltered, expectedCnt);
        Condition<Long> expectedCount7 = tester.tupleCount(intSpFiltered, expectedCnt);
        Condition<Long> expectedCount8 = tester.tupleCount(intSpDFiltered, expectedCnt);
        Condition<Long> expectedCount9 = tester.tupleCount(longSpFiltered, expectedCnt);
        Condition<Long> expectedCount10 = tester.tupleCount(longSpDFiltered, expectedCnt);
        Condition<Long> expectedCount11 = tester.tupleCount(floatSpFiltered, expectedCnt);
        Condition<Long> expectedCount12 = tester.tupleCount(floatSpDFiltered, expectedCnt);
        Condition<Long> expectedCount13 = tester.tupleCount(doubleSpFiltered, expectedCnt);
        Condition<Long> expectedCount14 = tester.tupleCount(doubleSpDFiltered, expectedCnt);
        
        complete(tester, expectedCount0, 15, TimeUnit.SECONDS);

        assertTrue(expectedCount1.toString(), expectedCount1.valid());
        assertTrue(expectedCount2.toString(), expectedCount2.valid());
        assertTrue(expectedCount3.toString(), expectedCount3.valid());
        assertTrue(expectedCount4.toString(), expectedCount4.valid());
        assertTrue(expectedCount5.toString(), expectedCount5.valid());
        assertTrue(expectedCount6.toString(), expectedCount6.valid());
        assertTrue(expectedCount7.toString(), expectedCount7.valid());
        assertTrue(expectedCount8.toString(), expectedCount8.valid());
        assertTrue(expectedCount9.toString(), expectedCount9.valid());
        assertTrue(expectedCount10.toString(), expectedCount10.valid());
        assertTrue(expectedCount11.toString(), expectedCount11.valid());
        assertTrue(expectedCount12.toString(), expectedCount12.valid());
        assertTrue(expectedCount13.toString(), expectedCount13.valid());
        assertTrue(expectedCount14.toString(), expectedCount14.valid());
    }
    
    private static void myAssertEquals(String s, Object expected, Object actual) {
        // avoid need for junit in the executing topology
        if ((expected == null && actual != null )
                || !expected.equals(actual))
            throw new java.lang.AssertionError(s + " expected=" + expected + " actual=" + actual);
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
    private static Supplier<Integer> sourceSupplier(final Supplier<Integer> someInt, final Integer expected) {
        return new Supplier<Integer>() {
            private transient Iterator<Integer> iter;
            @Override
            public Integer get() {
                myAssertEquals("SP value", expected, someInt.get());
                if (iter == null) {
                    iter = Arrays.asList(1, 2, 3, 4, 5).iterator();
                }
                if (iter.hasNext())
                    return iter.next();
                else
                    return null;
            }
        };
    }

    @SuppressWarnings("serial")
    private static Supplier<Iterable<Integer>> sourceIterableSupplier(final Supplier<Integer> someInt, final Integer expected) {
        return new Supplier<Iterable<Integer>>() {
            boolean done;
            @Override
            public Iterable<Integer> get() {
                myAssertEquals("SP value", expected, someInt.get());
                if (!done) {
                    done = true;
                    return Arrays.asList(1, 2, 3, 4, 5);
                }
                else
                    return Collections.emptyList();
            }
        };
    }

    @SuppressWarnings("serial")
    private static <U extends Serializable> Predicate<Integer> predicateFn(final Supplier<U> someU, final U expected) {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer v) {
                myAssertEquals("SP value", expected, someU.get());
                return true;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static UnaryOperator<Integer> unaryFn(final Supplier<Integer> someInt, final Integer expected) {
        return new UnaryOperator<Integer>() {
            @Override
            public Integer apply(Integer v) {
                myAssertEquals("SP value", expected, someInt.get());
                return v;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static Function<Integer,Integer> functionFn(final Supplier<Integer> someInt, final Integer expected) {
        return new Function<Integer,Integer>() {

            @Override
            public Integer apply(Integer v) {
                myAssertEquals("SP value", expected, someInt.get());
                return v;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static Function<List<Integer>,Integer> functionListFn(final Supplier<Integer> someInt, final Integer expected) {
        return new Function<List<Integer>,Integer>() {

            @Override
            public Integer apply(List<Integer> v) {
                myAssertEquals("SP value", expected, someInt.get());
                if (v.size() > 0)
                    return v.get(0);
                return null;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static Function<Integer,Iterable<Integer>> functionIterableFn(final Supplier<Integer> someInt, final Integer expected) {
        return new Function<Integer,Iterable<Integer>>() {

            @Override
            public Iterable<Integer> apply(Integer v) {
                myAssertEquals("SP value", expected, someInt.get());
                return Arrays.asList(v);
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static BiFunction<Integer, List<Integer>, Integer> biFunctionListFn(final Supplier<Integer> someInt, final Integer expected) {
        return new BiFunction<Integer, List<Integer>, Integer>() {

            @Override
            public Integer apply(Integer v1, List<Integer> v2) {
                myAssertEquals("SP value", expected, someInt.get());
                return v1;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static BiFunction<Integer, Integer, Integer> biFunctionFn(final Supplier<Integer> someInt, final Integer expected) {
        return new BiFunction<Integer, Integer, Integer>() {

            @Override
            public Integer apply(Integer v1, Integer v2) {
                myAssertEquals("SP value", expected, someInt.get());
                return v1;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static ToIntFunction<Integer> toIntFn(final Supplier<Integer> someInt, final Integer expected) {
        return new ToIntFunction<Integer>() {

            @Override
            public int applyAsInt(Integer tuple) {
                myAssertEquals("SP value", expected, someInt.get());
                return tuple;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static Consumer<Integer> sinkerFn(final Supplier<Integer> someInt, final Integer expected) {
        return new Consumer<Integer>() {

            @Override
            public void accept(Integer v) {
                myAssertEquals("SP value", expected, someInt.get());
            }
        };
    }
    
}
