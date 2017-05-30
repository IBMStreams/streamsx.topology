/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static java.util.Objects.requireNonNull;

import java.net.MalformedURLException;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streamsx.topology.function.FunctionContainer;
import com.ibm.streamsx.topology.function.FunctionContext;

class FunctionOperatorContext implements FunctionContext {
    
    private final OperatorContext context;
    private final FunctionContainer container;
    
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
    public Metric createCustomMetric(String name, String description, Kind kind) {
        return context.getMetrics().createCustomMetric(
                requireNonNull(name), requireNonNull(description), requireNonNull(kind));
    }
}
