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

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.BiFunction;
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
}
