package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.context.remote.RemoteContextFactory;

public class ZippedToolkitStreamsContext extends ToolkitStreamsContext {
	
    @Override
    public Type getType() {
        return Type.BUILD_ARCHIVE;
    }
	
	@Override
	Future<File> _submit(Topology app, Map<String, Object> config) throws Exception {
	    	    
	    JsonObject submission = createSubmission(app, config);
	    
	    return _submit(submission);
	}
	
	@Override
	Future<File> _submit(JsonObject submission) throws Exception {
	    // Let the remote archive do all the work.
	    @SuppressWarnings("unchecked")
        RemoteContext<File> ztrc = (RemoteContext<File>) RemoteContextFactory.getRemoteContext(RemoteContext.Type.BUILD_ARCHIVE);
	    
	    return ztrc.submit(submission);
	}
	
	@Override
    protected void makeToolkit(JsonObject deploy, File toolkitRoot) throws InterruptedException, Exception{
        // Do nothing
    }

}
