package com.ibm.streamsx.topology.test.api;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function7.Function;

public class IsolateTest {
    @Test
    public void multipleIsolationTest() throws Exception{
        Topology topology = new Topology("isolationTest");

        TStream<String> ss = topology.strings("hello", "world");
        TStream<String> ss0 = ss.isolate();
        TStream<String> ss1 = ss0.transform(getF(1), String.class);
        TStream<String> ss2 = ss1.isolate().transform(getF(2), String.class).transform(getF(3), String.class);
        
        TStream<String> ss3 = ss.transform(getF(4), String.class).isolate();
        TStream<String> ss4 = ss3.transform(getF(5), String.class).isolate();
        TStream<String> ss5 = ss4.transform(getF(6), String.class).isolate();
        TStream<String> ss6 = ss5.transform(getF(7), String.class);
        
        TStream<String> ss7 = ss3.transform(getF(8), String.class);
        
        Set<TStream<String>> set = new HashSet<>();
        set.add(ss6);
        set.add(ss2);
        
        TStream<String> union = ss7.union(set);
        
        union.print();
        
        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT).submit(topology).get();
    }
    
    @Test(expected = IllegalStateException.class)
    public void multipleIsolationExceptionTest() throws Exception{
        Topology topology = new Topology("isolationTest");

        TStream<String> ss = topology.strings("hello", "world");
        TStream<String> ss0 = ss.isolate();
        TStream<String> ss1 = ss0.transform(getF(1), String.class);
        ss1.isolate().transform(getF(2), String.class).transform(getF(3), String.class).print();

        
        TStream<String> ss3 = ss.transform(getF(4), String.class).isolate();
        TStream<String> ss4 = ss3.transform(getF(5), String.class).isolate();
        TStream<String> ss5 = ss4.transform(getF(6), String.class).isolate();
        ss5.transform(getF(7), String.class).print();

        TStream<String> ss7 = ss3.transform(getF(8), String.class);

        ss7.union(ss4).print();

        StreamsContextFactory.getStreamsContext(StreamsContext.Type.TOOLKIT).submit(topology).get();
    }

    @SuppressWarnings("serial")
    private static Function<String, String> getF(final int count){
        return new Function<String, String>(){
            int counter = 0;
            @Override
                public String apply(String v) {
                // TODO Auto-generated method stub
                return "Not Isolated" + Integer.toString(count) + " " + Integer.toString(counter++);
            }

        };

    }
}
