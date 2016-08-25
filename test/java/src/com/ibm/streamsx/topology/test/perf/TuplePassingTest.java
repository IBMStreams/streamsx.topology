/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.perf;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.test.TestTopology;

public class TuplePassingTest extends TestTopology {

    @Test
    public void testStringMillion5() throws Exception {
        assumeTrue(PERF_OK);
        assumeTrue(isMainRun());

        Topology t = new Topology("t1m5String");

        System.err.println("String");
        addTimer(stringWorkload(stringSource(t, 1000000), 5));
        StreamsContextFactory.getEmbedded().submit(t).get();
    }

    @Test
    public void testObjectMillion5() throws Exception {
        assumeTrue(PERF_OK);
        assumeTrue(isMainRun());

        Topology t = new Topology("t1m5Object");

        System.err.println("Object(TestValue)");
        addTimer(objectWorkload(objectSource(t, 1000000), 5));
        StreamsContextFactory.getEmbedded().submit(t).get();
    }

    @Test
    public void testStringMillion5Standalone() throws Exception {
        assumeTrue(SC_OK && PERF_OK);
        assumeTrue(isMainRun());

        Topology t = new Topology("t1m5StringStandalone");

        System.err.println("String-Standalone");
        addTimer(stringWorkload(stringSource(t, 1000000), 5));
        StreamsContextFactory.getStreamsContext(Type.STANDALONE).submit(t).get();
    }

    @Test
    public void testObjectMillion5Standalone() throws Exception {
        assumeTrue(SC_OK && PERF_OK);
        assumeTrue(isMainRun());

        Topology t = new Topology("t1m5ObjectStandalone");

        System.err.println("Object(TestValue)-Standalone");
        addTimer(objectWorkload(objectSource(t, 1000000), 5));
        StreamsContextFactory.getStreamsContext(Type.STANDALONE).submit(t)
                .get();
    }

    public static TStream<String> stringSource(Topology t, final int n) {
        return t.limitedSourceN(new Function<Long, String>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(Long c) {

                return "Better three hours too soon than a minute too late."
                        + c;
            }
        }, n);
    }

    public static TStream<String> emptyFilter(TStream<String> input) {

        return input.filter(new Predicate<String>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(String tuple) {
                return !tuple.isEmpty();
            }
        });
    }

    @SuppressWarnings("serial")
    public static <T> void addTimer(TStream<T> input) {

        input.sink(new Consumer<T>() {

            private transient int count;
            private transient long ts;

            @Override
            public void accept(T v) {
                if (++count % 10000 == 0) {
                    long now = System.currentTimeMillis();
                    long diffMs = now - ts;
                    System.err.println(count + ", " + diffMs);

                    ts = System.currentTimeMillis();
                }
            }

            private void readObject(java.io.ObjectInputStream stream)
                    throws IOException, ClassNotFoundException {
                stream.defaultReadObject();
                ts = System.currentTimeMillis();
            }
        });
    }

    @SuppressWarnings("serial")
    public static TStream<String> stringWorkload(TStream<String> input,
            int repeat) {

        // Add a chain of empty filters.
        for (int i = 0; i < repeat; i++) {

            input = emptyFilter(input);

            input = input.transform(new Function<String, String>() {

                @Override
                public String apply(String v1) {
                    return v1.replace('e', 'E');
                }
            });

            input = input.transform(new Function<String, String>() {

                @Override
                public String apply(String v1) {
                    return v1.replace('E', 'e');
                }
            });
        }
        return input;

    }

    public static TStream<TestValue> objectSource(Topology t, final int n) {
        return t.limitedSourceN(new Function<Long, TestValue>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public TestValue apply(Long c) {

                TestValue tv = new TestValue(c);

                return tv;
            }
        }, n);
    }

    @SuppressWarnings("serial")
    public static TStream<TestValue> objectWorkload(TStream<TestValue> input,
            int repeat) {

        // Add a chain of empty filters.
        for (int i = 0; i < repeat; i++) {

            input = input.filter(new Predicate<TestValue>() {

                @Override
                public boolean test(TestValue tuple) {
                    return tuple.i != 999;
                }
            });

            input = input.transform(new Function<TestValue, TestValue>() {

                @Override
                public TestValue apply(TestValue v1) {
                    TestValue v2 = new TestValue(v1);
                    v2.i += 37;
                    v2.l += 9835435l;
                    return v2;
                }
            });

            input = input.transform(new Function<TestValue, TestValue>() {

                @Override
                public TestValue apply(TestValue v1) {
                    TestValue v2 = new TestValue(v1);
                    v2.i -= 37;
                    v2.l -= 9835435l;
                    return v2;
                }
            });

        }
        return input;

    }

}
