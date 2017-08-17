/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static java.util.Objects.requireNonNull;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streamsx.topology.function.FunctionContainer;
import com.ibm.streamsx.topology.function.FunctionContext;

class FunctionOperatorContext implements FunctionContext {
    
    private final OperatorContext context;
    private final FunctionContainer container;
    
    private List<Runnable> metrics;
    private ScheduledFuture<?> metricsGetter;
    
    FunctionOperatorContext( OperatorContext context) {
        this.context = context;
        container = new FunctionPEContainer(context.getPE());
    }
    
    @Override
    public FunctionContainer getContainer() {
        return container;
    }

    @Override
    public ScheduledExecutorService getScheduledExecutorService() {
        return context.getScheduledExecutorService();
    }

    @Override
    public ThreadFactory getThreadFactory() {
        return context.getThreadFactory();
    }

    @Override
    public int getChannel() {
        return context.getChannel();
    }

    @Override
    public int getMaxChannels() {
        return context.getMaxChannels();
    }

    @Override
    public void addClassLibraries(String[] libraries)
            throws MalformedURLException {
        context.addClassLibraries(libraries);
    }
    
    @Override
    public synchronized void createCustomMetric(String name, String description, String kind, LongSupplier value) {
        
        LongSupplier supplier = requireNonNull(value);
        Metric cm = context.getMetrics().createCustomMetric(
                requireNonNull(name),
                requireNonNull(description),
                Metric.Kind.valueOf(kind.toUpperCase(Locale.US)));
        cm.setValue(supplier.getAsLong());
        
        if (metrics == null) {
            metrics = new ArrayList<>();
            
            metricsGetter = getScheduledExecutorService().scheduleWithFixedDelay(this::updateMetrics,
                    1, 1, TimeUnit.SECONDS);
        }
        
        metrics.add(() -> cm.setValue(supplier.getAsLong()));
    }
    
    private void updateMetrics() {
        for (Runnable mu : metrics)
            mu.run();
    }
    
    synchronized void finalMarkers() {
        if (metricsGetter != null) {
            metricsGetter.cancel(false);
            
            // Final update of the metrics
            updateMetrics();
        }
    }
    
    @Override
    public Set<String> getCustomMetricNames() {
        // TODO Auto-generated method stub
        return Collections.unmodifiableSet(context.getMetrics().getCustomMetrics().keySet());
    } 
}
