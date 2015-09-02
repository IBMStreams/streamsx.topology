package com.ibm.streamsx.topology.internal.functional.ops;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streamsx.topology.function.FunctionContainer;

class FunctionPEContainer implements FunctionContainer {
    
    private final ProcessingElement pe;
    
    FunctionPEContainer(ProcessingElement pe) {
        this.pe = pe;
    }

    @Override
    public String getJobId() {
        return pe.getJobId().toString();
    }

    @Override
    public String getId() {
        return pe.getPEId().toString();
    }

    @Override
    public String getDomainId() {
        return pe.getDomainId();
    }

    @Override
    public String getInstanceId() {
        return pe.getInstanceId();
    }

    @Override
    public int getRelaunchCount() {
        return pe.getRelaunchCount();
    }

    @Override
    public InetAddress getConfiguredHost() throws UnknownHostException {
        return pe.getConfiguredHost();
    }

}
