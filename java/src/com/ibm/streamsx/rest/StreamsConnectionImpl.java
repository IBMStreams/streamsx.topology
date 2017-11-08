package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;

public class StreamsConnectionImpl extends AbstractStreamsConnection {

    final private String userName;

    StreamsConnectionImpl(String userName, String authorization,
            String resourcesUrl, boolean allowInsecure) throws IOException {
        super(authorization, resourcesUrl, allowInsecure);
        this.userName = userName;
    }

    @Override
    protected String getAuthorization() {
        return authorization;
    }

    /* (non-Javadoc)
     * @see com.ibm.streamsx.rest.StreamsConnection#cancelJob(java.lang.String)
     */
    @Override
    public boolean cancelJob(String jobId) throws Exception {
        InvokeCancel cancelJob = new InvokeCancel(new BigInteger(jobId), userName);
        return cancelJob.invoke(false) == 0;
    }
}
