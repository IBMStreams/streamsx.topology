/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.spl;

import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streams.operator.window.StreamWindow.Policy;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.internal.core.WindowDefinition;

class SPLWindowImpl extends WindowDefinition<Tuple,Object> implements SPLWindow {

    private final StreamWindow.Policy triggerPolicy;
    private final long triggerConfig;
    private final TimeUnit triggerTimeUnit;

    SPLWindowImpl(TWindow<Tuple,?> window, int count) {
        super(window.getStream(), window);
        this.triggerPolicy = Policy.COUNT;
        this.triggerConfig = count;
        triggerTimeUnit = null;
    }

    SPLWindowImpl(TWindow<Tuple,?> window, long time, TimeUnit unit) {
        super(window.getStream(), window);
        this.triggerPolicy = Policy.TIME;
        this.triggerConfig = time;
        triggerTimeUnit = unit;
    }

    @Override
    public SPLStream getStream() {
        return (SPLStream) super.getStream();
    }

    /**
     * Make the passed input port windowed.
     */
    void windowInput(BInputPort inputPort) {
        inputPort.window(StreamWindow.Type.SLIDING.name(), policy, config, this.timeUnit,
                triggerPolicy.name(), triggerConfig, triggerTimeUnit, false);
    }
}
