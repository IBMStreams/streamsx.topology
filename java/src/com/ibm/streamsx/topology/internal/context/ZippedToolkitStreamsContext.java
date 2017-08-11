package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.context.remote.RemoteContextFactory;

public class ZippedToolkitStreamsContext extends ToolkitStreamsContext {
	
	private final boolean keepBuildArchive;

	public ZippedToolkitStreamsContext() {
        this.keepBuildArchive = false;
    }
	
    public ZippedToolkitStreamsContext(boolean keepBuildArchive) {
        this.keepBuildArchive = keepBuildArchive;
    }
	
    @Override
    public Type getType() {
        return Type.BUILD_ARCHIVE;
    }
	
	@Override
	protected Future<File> action(AppEntity entity) throws Exception {
	    // Let the remote archive do all the work.
	    @SuppressWarnings("unchecked")
        RemoteContext<File> ztrc = (RemoteContext<File>) RemoteContextFactory.getRemoteContext(RemoteContext.Type.BUILD_ARCHIVE, keepBuildArchive);
	    
	    return ztrc.submit(entity.submission);
	}
	
	@Override
    protected void makeToolkit(JsonObject deploy, File toolkitRoot) throws InterruptedException, Exception{
        // Do nothing
    }

}
