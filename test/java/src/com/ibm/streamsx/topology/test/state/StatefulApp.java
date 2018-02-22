/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018 
 */
package com.ibm.streamsx.topology.test.state;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Initializable;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;

/**
 * Testing stateful app that uses checkpointing.
 * 
 * As well as being used as a test it can be launched manually
 * using this class as the Java main class.
 *
 */
public class StatefulApp {

    public static void main(String[] args) throws InterruptedException, ExecutionException, Exception {
        
        String contextType = Type.DISTRIBUTED.name();
        
        if (args.length == 1)
            contextType = args[0];
        
        Topology topology = new Topology();
        
        createApp(topology, false, true);
        
        Map<String,Object> config = new HashMap<>();

        @SuppressWarnings("unchecked")
        StreamsContext<BigInteger> context =
             (StreamsContext<BigInteger>) StreamsContextFactory.getStreamsContext(contextType);
        
        BigInteger jobId = context.submit(topology, config).get();

        System.out.println("Submitted job with jobId=" + jobId);
    }
    
    
    public static TStream<Long> createApp(Topology topology, boolean checkpoint, boolean fail) { 

        if (checkpoint)
            topology.checkpointPeriod(5, TimeUnit.SECONDS);
        
        TStream<Long> s = topology.periodicSource(new Stateful.Counter(fail), 21, TimeUnit.MILLISECONDS);
        s = s.isolate().filter(new Stateful.Filter(fail));
        s = s.isolate().map(new Stateful.Map(fail));
        s.isolate().forEach(new Stateful.ForEach(fail));
        
        return s;
    }
    
    static abstract class Stateful implements Initializable, Serializable {
        private static final long serialVersionUID = 1L;
        
        private final AtomicLong start = new AtomicLong();
        private final AtomicLong restart = new AtomicLong();
        private final AtomicLong sequence = new AtomicLong();
        
        private final boolean fail;
        private final int failWhen;
        private transient FunctionContext functionContext;
        
        Stateful(boolean fail) {
            this.fail = fail;
            failWhen = fail && allowFailWhen() ? (5*50) + (int) (((double) (20*50)) * Math.random()) : -1;
        }
        
        boolean allowFailWhen() {
            return true;
        }
        
        @Override
        public void initialize(FunctionContext functionContext) throws Exception {
            this.functionContext = functionContext;
            
            if (fail && functionContext.getContainer().getRelaunchCount() == 0)
                throw new Exception("Injected initial Test failure" + this.getClass().getName());
                            
            final long now = System.currentTimeMillis();
            if (start.get() == 0)
                start.set(now);
            restart.set(now);
            
            functionContext.createCustomMetric("start", "Job start time", "time", start::get);
            functionContext.createCustomMetric("restart", "PE restart time", "time", restart::get);
            functionContext.createCustomMetric("sequence", "", "counter", sequence::get);
            functionContext.createCustomMetric("fail_at", "", "counter", () -> failWhen);
        }
        
        long bump() {
            if (fail && sequence.get() == failWhen) {
                if (functionContext.getContainer().getRelaunchCount() == 1)
                    throw new RuntimeException("Injected Test failure@" + failWhen + " : "+ this.getClass().getName());                
            }
            return sequence.getAndIncrement();
        }
        
        static class Counter extends Stateful implements Supplier<Long> {
            private static final long serialVersionUID = 1L;
            
            Counter(boolean fail) {
                super(fail);
            }
            
            boolean allowFailWhen() {
                return false;
            }         
            
            @Override
            public Long get() {
                return bump();
            }
        }
        
        static class Filter extends Stateful implements Predicate<Long> {
            private static final long serialVersionUID = 1L;
            Filter(boolean fail) {
                super(fail);
            }
            @Override
            public boolean test(Long tuple) {
                bump();
                return true;
            }           
        }
        
        static class Map extends Stateful implements Function<Long, Long> {
            private static final long serialVersionUID = 1L;
            
            Map(boolean fail) {
                super(fail);
            }

            @Override
            public Long apply(Long v) {
                bump();
                return v;
            }        
        }
        static class ForEach extends Stateful implements Consumer<Long> {
            private static final long serialVersionUID = 1L;
            
            ForEach(boolean fail) {
                super(fail);
            }

            @Override
            public void accept(Long v) {
                bump();
            }      
        }
    }
}
