/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streamsx.topology.TKeyedStream;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;

public class JoinTest extends TestTopology {


    @Test
    public void testJoin() throws Exception {
        final Topology t = new Topology();
        TStream<String> strings = t.strings("a", "b", "c", "d", "e");
        
        TWindow<String,?> window = strings.last(3);
        
        TStream<Number> main = t.numbers(0,134,76);
        main = main.throttle(1, TimeUnit.SECONDS);
        
        TStream<List<String>> joined = _jointest(main, window);
        TStream<String> asString = StringStreams.toString(joined);
        
        completeAndValidate(asString, 12, "[c-134, d-134, e-134]", "[c-76, d-76, e-76]");
    }
    
    @Test
    public void testEmptyJoin() throws Exception {
        final Topology t = new Topology();
        TStream<String> strings = t.strings();
        
        TWindow<String,?> window = strings.last(3);
        
        TStream<Number> main = t.numbers(0,134,76);
        main = main.throttle(1, TimeUnit.SECONDS);
        
        TStream<List<String>> joined = _jointest(main, window);
        TStream<String> asString = StringStreams.toString(joined);
        
        completeAndValidate(asString, 12, "[empty-134]", "[empty-76]");
    }
    
    private static TStream<List<String>> _jointest(TStream<Number> main, TWindow<String,?> window) {
        
        return main.join(window, new BiFunction<Number, List<String>, List<String>>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public List<String> apply(Number v1, List<String> v2) {
                // Skip the first value to ensure the window contents are stable.
                if (v1.intValue() == 0)
                   return null;
                
                if (v2.isEmpty())
                    return Collections.singletonList("empty-" + v1.toString());
                
                List<String> tuple = new ArrayList<>();
                for (String s : v2)
                    tuple.add(s + "-" + v1.toString());
                
                return tuple;
            }
        });        
    }

    @Test
    public void testJoinLast() throws Exception {
        final Topology t = new Topology();
        TStream<String> strings = t.strings("a", "b", "c", "d", "e");
        
        TStream<Number> main = t.numbers(0,134,76);
        main = main.throttle(1, TimeUnit.SECONDS);
        
        TStream<List<String>> joined = _joinLasttest(main, strings);
        TStream<String> asString = StringStreams.toString(joined);
        
        completeAndValidate(asString, 12, "[e-134]", "[e-76]");
    }
    
    @Test
    public void testEmptyJoinLast() throws Exception {
        final Topology t = new Topology();
        TStream<String> strings = t.strings();
        
        TStream<Number> main = t.numbers(0,134,76);
        main = main.throttle(1, TimeUnit.SECONDS);
        
        TStream<List<String>> joined = _joinLasttest(main, strings);
        TStream<String> asString = StringStreams.toString(joined);
        
        completeAndValidate(asString, 12, "[empty-134]", "[empty-76]");
    }
    
    private static TStream<List<String>> _joinLasttest(TStream<Number> main, TStream<String> other) {
        
        return main.joinLast(other, new BiFunction<Number, String, List<String>>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public List<String> apply(Number v1, String v2) {
                // Skip the first value to ensure the window contents are stable.
                if (v1.intValue() == 0)
                   return null;
                if (v2 == null)
                    return Collections.singletonList("empty-" + v1.toString());
                
                List<String> tuple = new ArrayList<>();
                tuple.add(v2 + "-" + v1.toString());
                
                return tuple;
            }
        });        
    }
    
    @Test
    public void testKeyedJoin() throws Exception {
        final Topology t = new Topology();
        TKeyedStream<String,String> strings = t.strings("a", "b", "c", "a", "a", "c").key();
        
        TKeyedStream<String,String> main = delayedList(t, "a", "b", "c", "d").key();
        
        TStream<Integer> joined = _testKeyedJoin(main, strings.last(3));
        TStream<String> asString = StringStreams.toString(joined);
        
        completeAndValidate(asString, 25, "3", "1", "2", "0");
    }
    
    private static TStream<String> delayedList(Topology t, String ...strings) {
        
        final ArrayList<String> data = new ArrayList<>();
        for (String s : strings)
            data.add(s);
        return t.source(new Supplier<Iterable<String>>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> get() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return null;
                }
                
                return data ;
            }});
    }
    
    private static TStream<Integer> _testKeyedJoin(TKeyedStream<String,String> main, TWindow<String,String> window) {
        
        return main.join(window, new BiFunction<String, List<String>, Integer>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer apply(String v1, List<String> v2) {
                for (String wt : v2)
                    if (!v1.equals(wt))
                        return -1;
                return v2.size();
            }
        });        
    }
    
    @Test
    public void testKeyedJoinLast() throws Exception {
        final Topology t = new Topology();
        TKeyedStream<String,String> strings = firstChar(t.strings("a1", "b1", "c1", "a2", "a3", "c2"));
        
        TKeyedStream<String,String> main = delayedList(t, "a", "b", "c", "d").key();
        
        TStream<String> joined = _testKeyedJoinLast(main, strings);
        TStream<String> asString = StringStreams.toString(joined);
        
        completeAndValidate(asString, 25, "a3", "b1", "c2", "empty");
    }
    
    private static TKeyedStream<String,String> firstChar(TStream<String> strings) {
        return strings.key(new Function<String,String>() {

            @Override
            public String apply(String v) {
                return v.substring(0, 1);
            }});
    }

    private static TStream<String> _testKeyedJoinLast(TKeyedStream<String,String> main, TKeyedStream<String,String> strings) {
        
        return main.joinLast(strings, new BiFunction<String, String, String>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(String v1, String v2) {
                if (v2 == null)
                    return "empty";
                
                return v2;
            }
        });        
    }
}
