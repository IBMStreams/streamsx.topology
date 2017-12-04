package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;

class StreamsConnectionImpl extends AbstractStreamsConnection {

    private final String userName;

    StreamsConnectionImpl(String userName, String authorization,
            String resourcesUrl, boolean allowInsecure) throws IOException {
        super(authorization, resourcesUrl, allowInsecure);
        this.userName = userName;
    }

    @Override
    String getAuthorization() {
        return authorization;
    }

    @Override
    boolean cancelJob(Instance instance, String jobId) throws IOException {

        InvokeCancel cancelJob = new InvokeCancel(
                instance.getDomain().getId(), instance.getId(),
                new BigInteger(jobId), userName);
        try {
            return cancelJob.invoke(false) == 0;
        } catch (Exception e) {
            throw new RESTException("Unable to cancel job " + jobId
                    + " in instance " + instance.getId(), e);
        }
    }
}
