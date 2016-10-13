package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities.gson;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.context.remote.RemoteContextFactory;
import com.ibm.streamsx.topology.internal.context.remote.ZippedToolkitRemoteContext;

public class ZippedToolkitStreamsContext extends ToolkitStreamsContext {
	
    @Override
    public Type getType() {
        return Type.BUILD_ARCHIVE;
    }
	
	@Override
	public Future<File> submit(Topology app, Map<String, Object> config) throws Exception {        
        File toolkitRoot = super.submit(app, config).get();
        
        JSONObject deploy = new JSONObject();
        deploy.put(ContextProperties.TOOLKIT_DIR, toolkitRoot.getAbsolutePath());
        if (config.containsKey(ContextProperties.KEEP_ARTIFACTS))
            deploy.put(KEEP_ARTIFACTS, config.get(KEEP_ARTIFACTS));

        JSONObject submission = new JSONObject();
        submission.put(SUBMISSION_DEPLOY, deploy);
        submission.put(SUBMISSION_GRAPH, app.builder().complete());
        return ZippedToolkitRemoteContext.createCodeArchive(toolkitRoot, gson(submission));
	}
	
	@Override
	public Future<File> submit(JSONObject submission) throws Exception {
	    // Let the remote archive do all the work.
	    @SuppressWarnings("unchecked")
        RemoteContext<File> ztrc = (RemoteContext<File>) RemoteContextFactory.getRemoteContext(RemoteContext.Type.BUILD_ARCHIVE);
	    
	    return ztrc.submit(gson(submission));
	}
	
	@Override
    protected void makeToolkit(JSONObject deployInfo, File toolkitRoot) throws InterruptedException, Exception{
        // Do nothing
    }

}
