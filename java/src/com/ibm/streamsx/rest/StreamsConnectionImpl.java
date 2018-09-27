package com.ibm.streamsx.rest;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.streams.InvokeCancel;
import com.ibm.streamsx.topology.internal.streams.InvokeSubmit;

class StreamsConnectionImpl extends AbstractStreamsConnection {

	private final String authorization;
    private final String userName;

    StreamsConnectionImpl(String userName, String authorization,
            String resourcesUrl, boolean allowInsecure) {
        super(resourcesUrl, allowInsecure);
        this.userName = userName;
        this.authorization = authorization;
    }

    @Override
    String getAuthorization() {
        return authorization;
    }
    
    @Override
    ApplicationBundle uploadBundle(Instance instance, File bundle) throws IOException {
    	if (instance.domain == null)
    		return StreamsRestActions.uploadBundle(instance, bundle);

    	return super.uploadBundle(instance, bundle);
    }

    @Override
    boolean cancelJob(Instance instance, String jobId) throws IOException {
    	
    	if (instance.domain == null)
    		return StreamsRestActions.cancelJob(instance, jobId);

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
		
		if (jco == null)
			jco = new JsonObject();
		
    	if (bundle.instance().domain == null)
    		return StreamsRestActions.submitJob(bundle, jco);
		
		InvokeSubmit submit = new InvokeSubmit(((FileBundle)bundle).bundleFile());
		
		BigInteger jobId;
		try {
			jobId = submit.invoke(jco,
					bundle.instance().getDomain().getId(), bundle.instance().getId());
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		final String jobIds = jobId.toString();
		
		Instance instance = bundle.instance();
		
		JsonObject response = new JsonObject();
		response.addProperty(SubmissionResultsKeys.JOB_ID, jobIds);
		
		return new ResultImpl<Job, JsonObject>(true, jobIds,
				() -> instance.getJob(jobIds), response);
	}
}
