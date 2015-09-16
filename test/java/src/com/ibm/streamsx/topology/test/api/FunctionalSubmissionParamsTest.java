/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
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
    public void FilterTest() throws Exception {
        Topology topology = new Topology("FilterTest");
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
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
    public void SourceTest() throws Exception {
        Topology topology = new Topology("SourceTest");
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        Supplier<Integer> someInt = topology.createSubmissionParameter("someInt", Integer.class);
        Supplier<Integer> someIntD = topology.createSubmissionParameter("someIntD", 20);

        // note these SP Suppliers check and blow up if get the wrong value
        
        TStream<Integer> s = topology.source(sourceSupplier(someInt, 10));
        TStream<Integer> sD = topology.source(sourceSupplier(someIntD, 20));

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
    public void PeriodicSourceTest() throws Exception {
        Topology topology = new Topology("PeriodicSourceTest");
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
        Supplier<Integer> someInt = topology.createSubmissionParameter("someInt", Integer.class);
        Supplier<Integer> someIntD = topology.createSubmissionParameter("someIntD", 20);
        
        // note these SP Suppliers check and blow up if get the wrong value

        TStream<Integer> s = topology.periodicMultiSource(sourceSupplier(someInt, 10), 100, TimeUnit.MILLISECONDS);
        TStream<Integer> sD = topology.periodicMultiSource(sourceSupplier(someIntD, 20), 100, TimeUnit.MILLISECONDS);

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
    public void MiscTest() throws Exception {
        Topology topology = new Topology("MiscTest");
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
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

        // note these SP Suppliers check and blow up if get the wrong value
        
        TStream<Integer> modified = s.modify(unaryFn(someInt, 1));
        TStream<Integer> modifiedD = s.modify(unaryFn(someIntD, 2));
        
        TStream<Integer> xformed = s.transform(functionFn(someInt, 1));
        TStream<Integer> xformedD = s.transform(functionFn(someIntD, 2));

        // issue#210 NPE in WindowDefinition._joinInternal (not a SP problem)
//        TStream<Integer> joined = s.join(s.last(1), biFunctionFn(someInt, 1));
//        TStream<Integer> joinedD = s.join(s.last(1), biFunctionFn(someInt, 2));

        Map<String,Object> params = new HashMap<>();
        params.put("someInt", 1);
        getConfig().put(ContextProperties.SUBMISSION_PARAMS, params);
        
        ////////////////////
        
        Set<TStream<Integer>> all = new HashSet<>( Arrays.asList(
                modified, modifiedD,
                xformed, xformedD
//                joined, joinedD
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
    public void AllTypesTest() throws Exception {
        Topology topology = new Topology("AllTypesTest");
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        
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

        // note these SP Suppliers check and blow up if get the wrong value

        TStream<Integer> strSpFiltered = s.filter(strPredicateFn(strSp, "yo"));
        TStream<Integer> strSpDFiltered = s.filter(strPredicateFn(strSpD, "dude"));
        TStream<Integer> byteSpFiltered = s.filter(numPredicateFn(byteSp, (byte)1));
        TStream<Integer> byteSpDFiltered = s.filter(numPredicateFn(byteSpD, (byte)2));
        TStream<Integer> shortSpFiltered = s.filter(numPredicateFn(shortSp, (short)3));
        TStream<Integer> shortSpDFiltered = s.filter(numPredicateFn(shortSpD, (short)4));
        TStream<Integer> intSpFiltered = s.filter(numPredicateFn(intSp, 5));
        TStream<Integer> intSpDFiltered = s.filter(numPredicateFn(intSpD, 6));
        TStream<Integer> longSpFiltered = s.filter(numPredicateFn(longSp, (long)7));
        TStream<Integer> longSpDFiltered = s.filter(numPredicateFn(longSpD, (long)8));
        TStream<Integer> floatSpFiltered = s.filter(numPredicateFn(floatSp, 9.0f));
        TStream<Integer> floatSpDFiltered = s.filter(numPredicateFn(floatSpD, 10.0f));
        TStream<Integer> doubleSpFiltered = s.filter(numPredicateFn(doubleSp, 11.0d));
        TStream<Integer> doubleSpDFiltered = s.filter(numPredicateFn(doubleSpD, 12.0d));

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
    private static Supplier<Iterable<Integer>> sourceSupplier(final Supplier<Integer> someInt, final int expected) {
        return new Supplier<Iterable<Integer>>() {
           private Iterator<Integer> iter;
            @Override
            public Iterable<Integer> get() {
                return new Iterable<Integer>() {

                    @Override
                    public Iterator<Integer> iterator() {
                        if (iter==null) {
                            iter = new Iterator<Integer>() {
                                int cnt = 0;
                                @Override
                                public boolean hasNext() {
                                    if (someInt.get() != expected)
                                        throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someInt.get());
                                    return cnt < 5;
                                }

                                @Override
                                public Integer next() {
                                    if (cnt > 10000)
                                        throw new IllegalStateException("runaway?");
                                    return cnt++;
                                }
                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException("remove");
                                }

                            };
                        }
                        return iter;
                    }
                    
                };
            }
        };
    }

    // issue#209 prevents this from working.  Do non-generic variants instead
//    @SuppressWarnings("serial")
//    private static <U> Predicate<Integer> predicateFn(final Supplier<U> someU, final U expected) {
//        return new Predicate<Integer>() {
//            @Override
//            public boolean test(Integer v) {
//                if (!someU.get().equals(expected))
//                    throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someU.get());
//                return true;
//            }
//        };
//    }

    @SuppressWarnings("serial")
    private static Predicate<Integer> strPredicateFn(final Supplier<String> someU, final String expected) {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer v) {
                if (!someU.get().equals(expected))
                    throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someU.get());
                return true;
            }
        };
    }

    @SuppressWarnings("serial")
    private static Predicate<Integer> numPredicateFn(final Supplier<? extends Number> someU, final Number expected) {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer v) {
                if (!someU.get().equals(expected))
                    throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someU.get());
                return true;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static UnaryOperator<Integer> unaryFn(final Supplier<Integer> someInt, final int expected) {
        return new UnaryOperator<Integer>() {
            @Override
            public Integer apply(Integer v) {
                if (!someInt.get().equals(expected))
                    throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someInt.get());
                return v;
            }
        };
    }
    
    @SuppressWarnings("serial")
    private static Function<Integer,Integer> functionFn(final Supplier<Integer> someInt, final int expected) {
        return new Function<Integer,Integer>() {

            @Override
            public Integer apply(Integer v) {
                if (!someInt.get().equals(expected))
                    throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someInt.get());
                return v;
            }
        };
    }
    
    @SuppressWarnings({ "serial", "unused" })
    private static BiFunction<Integer, List<Integer>, Integer> biFunctionFn(final Supplier<Integer> someInt, final int expected) {
        return new BiFunction<Integer, List<Integer>, Integer>() {

            @Override
            public Integer apply(Integer v1, List<Integer> v2) {
                if (!someInt.get().equals(expected))
                    throw new IllegalStateException("Didn't get expected SP value.  Expected " + expected + " got " + someInt.get());
                return v1;
            }
        };
    }
}
