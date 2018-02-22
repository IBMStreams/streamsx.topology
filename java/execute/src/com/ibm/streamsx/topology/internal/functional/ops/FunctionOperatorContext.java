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
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streamsx.topology.function.FunctionContainer;
import com.ibm.streamsx.topology.function.FunctionContext;

class FunctionOperatorContext implements FunctionContext {
    
    private final OperatorContext context;
    private final FunctionContainer container;
    
    private List<MetricSetter> metrics;
    private ScheduledFuture<?> metricsGetter;
    
    static class MetricSetter {
        final Metric metric;
        final LongSupplier value;
        
        MetricSetter(Metric metric, LongSupplier value) {
            this.metric = metric;
            this.value = value;
        }
    }
    
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
        
        name = requireNonNull(name);
        description = requireNonNull(description);
        LongSupplier supplier = requireNonNull(value);
        Kind ekind = Metric.Kind.valueOf(kind.toUpperCase(Locale.US));
        
        Metric cm;
        try {
            cm = context.getMetrics().createCustomMetric(name, description, ekind);
        } catch (IllegalArgumentException e) {
            // See if this incarnation of the logic
            // already created it. If not then the operator
            // has been reset so the metric was created by a previous incarnation.
            // if so we use the existing metric and effectively rebind it to
            // the new value supplier.
            if (metrics != null) {
                synchronized (metrics) {
                    for (MetricSetter ms : metrics)
                        if (ms.metric.getName().equals(name))
                            throw e;
                }
            }
            cm = context.getMetrics().getCustomMetric(name);           
        }
        cm.setValue(supplier.getAsLong());
        
        if (metrics == null) {
            metrics = Collections.synchronizedList(new ArrayList<>());
            
            metricsGetter = getScheduledExecutorService().scheduleWithFixedDelay(this::updateMetrics,
                    1, 1, TimeUnit.SECONDS);
        }
        
        metrics.add(new MetricSetter(cm, value));
    }
    
    private void updateMetrics() {
        synchronized (metrics) {
            for (MetricSetter ms : metrics)
                ms.metric.setValue(ms.value.getAsLong());
        }
    }
    
    void clearMetrics() {
        if (metrics != null)
            metrics.clear();
    }
    
    synchronized void finalMarkers() {
        if (metricsGetter != null) {
            metricsGetter.cancel(false);
            metricsGetter = null;
            
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
