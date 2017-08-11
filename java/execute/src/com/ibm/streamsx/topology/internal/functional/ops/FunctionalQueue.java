/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;

class FunctionalQueue<T> implements StreamHandler<T>, Runnable {

    private BlockingQueue<T> queue;
    
    private final StreamHandler<T> handler;
    private final Thread reader;
        
    FunctionalQueue(OperatorContext context, int size, StreamHandler<T> handler) {
        this.queue = new ArrayBlockingQueue<>(size);
        this.handler = handler;

        reader = context.getThreadFactory().newThread(this);
        reader.setDaemon(false);
        start();
    }
    
    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                T tuple = queue.take();
                handler.tuple(tuple);

            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    
    
    @Override
    public synchronized void tuple(T tuple) throws Exception {
        queue.put(tuple);
    }
       
    /**
     * Ensure all tuples seen before the mark
     * are processed before the mark.
     */
    @Override
    public void mark(Punctuation mark) throws Exception {
        if (!queue.isEmpty()) {
            List<T> drained = new ArrayList<T>(queue.size());
            queue.drainTo(drained);
            if (!drained.isEmpty()) {
                for (T tuple : drained)
                    handler.tuple(tuple);
            }
        }
        
        handler.mark(mark);
    }
    
    void start() {
        reader.start();
    }
    
    void stop() {
        reader.interrupt();
    }
    
}
