package com.ibm.streamsx.topology.internal.functional.ops;

import java.net.MalformedURLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streamsx.topology.function.FunctionContext;

class FunctionOperatorContext implements FunctionContext {
    
    private final OperatorContext context;
    
    FunctionOperatorContext( OperatorContext context) {
        this.context = context;
    }

    @Override
    public ProcessingElement getPE() {
        return context.getPE();
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
}
