package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.streams.InvokeCancel;
import com.ibm.streamsx.topology.internal.streams.InvokeSubmit;

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

	@Override
	Result<Job, JsonObject> submitJob(ApplicationBundle bundle, JsonObject jco) throws IOException {
		InvokeSubmit submit = new InvokeSubmit(((FileBundle)bundle).bundleFile());
		
		if (jco == null)
			jco = new JsonObject();
		
		//JsonObject deploy = new JsonObject();
		BigInteger jobId;
		try {
			jobId = submit.invoke(jco);
			System.out.println("SUBMITTED:" +jobId);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		final String jobIds = jobId.toString();
		
		Instance instance = bundle.instance();
		
		return new ResultImpl<Job, JsonObject>(true, jobIds,
				() -> instance.getJob(jobIds), new JsonObject());
		
		
		// TODO Auto-generated method stub
		// throw new UnsupportedOperationException();
	}
}
