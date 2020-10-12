/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * This context uses the Streams REST API of the Build Service to build an application,
 * and the CP4D Jobs/Spaces REST API to submit the built application. This context does not 
 * download the artifacts, i.e. the application bundle.
 * 
 * @since topology 1.17
 */
public class DistributedCp4dRestContext extends BuildServiceContext {
    
    public DistributedCp4dRestContext() {
        super(/*downloadArtifacts=*/false);
    }

    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }

    @Override
    protected void postBuildAction (JsonObject deploy, JsonObject jco, JsonObject result) throws Exception {
        // here we submit the Job with the SAB in the artifacts URL
        report("Submitting job");
        
        JsonArray applicationBundles = new JsonArray();
        JsonObject application = new JsonObject();

        JsonArray artifacts = GsonUtilities.array(GsonUtilities.object(result, "build"), "artifacts");
        JsonObject artifact0 = (JsonObject)artifacts.get(0);

        String sabUrl = artifact0.get("sabUrl").getAsString();
        System.out.println ("SAB-URL = " + sabUrl);
        
        
        application.addProperty("application", sabUrl);
        JsonObject applicationCredentials = new JsonObject();
        final String token = StreamsKeys.getBearerToken(deploy);
        applicationCredentials.addProperty("bearerToken", token);
        application.add("applicationCredentials", applicationCredentials);
        applicationBundles.add (application);
        
        try {
            report("Job id:" + "TODO");
        }
        finally {
//            if (!GsonUtilities.jboolean(deploy, KEEP_ARTIFACTS)) {
                deleteBuilds (getApplicationBuild());
//            }
        }
    }
}
