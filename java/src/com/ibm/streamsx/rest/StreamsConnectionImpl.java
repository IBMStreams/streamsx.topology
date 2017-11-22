package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;
import com.ibm.streamsx.topology.internal.streams.Util;

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
    boolean cancelJob(String instanceId, String jobId) throws IOException {
        // Sanity check instance since InvokeCancel uses default instance
        if (!Util.getDefaultInstanceId().equals(instanceId)) {
            throw new RESTException("Unable to cancel job in instance " + instanceId);
        }
        InvokeCancel cancelJob = new InvokeCancel(new BigInteger(jobId), userName);
        try {
            return cancelJob.invoke(false) == 0;
        } catch (Exception e) {
            throw new RESTException("Unable to cancel job " + jobId
                    + " in instance " + instanceId, e);
        }
    }
}
