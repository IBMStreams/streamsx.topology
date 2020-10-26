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
package com.ibm.streamsx.rest.internal.icp4d;

import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import com.ibm.streamsx.rest.internal.AbstractConnection;

/**
 * This class represents an entity->configuration object of a JobRun REST resource from the /v2/jobs API of the CP4D.
 * 
 * @since 1.17
 */
public class JobRunConfiguration extends Element {
//  {
//  "entity": {
//    "job_run": {
// ==>  "configuration": {
//        "application": "https://cluste.ip:443/streams_build_service/v1/namespaces/nbgr55/instances/sample-streams/builds/122/artifacts/0/applicationbundle",
//        "env_type": "streams",
//        "env_variables": [],
//        "jobConfigurationOverlay": {
//          "jobConfigOverlays": [
//            {
//              "deploymentConfig": {
//                "parallelRegionConfig": {
//                  "fusionType": "channelIsolation"
//                }
//              },
//              "jobConfig": {
//                "jobName": "MyTestJob"
//              }
//            }
//          ]
//        },
//        "jobId": "102",
//        "namespace": "nbgr55",
//        "serviceInstance": "1602644000868858",
//        "streamsInstance": "sample-streams"
//      },
//      "isScheduledRun": false,
//      "job_name": "namespace1__TestApplication1",
//      "job_ref": "6fb3a4ec-fd1e-462d-bb72-32e82aeee713",
//      "job_type": "streams",
//      "space_name": "rolefs space",
//      "state": "Running"
//    }
//  },
//  "href": "/v2/assets/31c35366-3d79-4c96-ae22-bfa44ce6d711?space_id=54b262a0-cf67-4ac9-82bb-69caa8cabcae",
//  "metadata": {
//    "asset_attributes": [
//      "job_run"
//    ],
//    "asset_category": "USER",
//    "asset_id": "31c35366-3d79-4c96-ae22-bfa44ce6d711",
//    "asset_state": "available",
//    "asset_type": "job_run",
//    "catalog_id": "880ab3eb-a7c6-4155-8341-5f9513fb6838",
//    "created": 1602768721478,
//    "created_at": "2020-10-15T13:32:01Z",
//    "description": "created by streamsx.topology for namespace1__TestApplication1 application submissions",
//    "name": "MyTestJob",
//    "origin_country": "us",
//    "owner_id": "1000330999",
//    "rating": 0,
//    "rov": {
//      "collaborator_ids": {},
//      "mode": 0
//    },
//    "size": 0,
//    "space_id": "54b262a0-cf67-4ac9-82bb-69caa8cabcae",
//    "tags": [],
//    "total_ratings": 0,
//    "usage": {
//      "access_count": 0,
//      "last_access_time": 1602768734098,
//      "last_accessed_at": "2020-10-15T13:32:14Z",
//      "last_accessor_id": "1000330999",
//      "last_update_time": 1602768734098,
//      "last_updated_at": "2020-10-15T13:32:14Z",
//      "last_updater_id": "1000330999"
//    },
//    "version": 2
//  }
//}
    @Expose private String jobId;
    @Expose private String application;
    @Expose private String streamsInstance;
 
    /**
     * create an object from a JSON object
     * @param connection
     * @param gsonString
     * @return
     */
    static final JobRunConfiguration create (AbstractConnection connection, JsonObject gsonString) {
        JobRunConfiguration element = gson.fromJson (gsonString, JobRunConfiguration.class);
        element.setConnection (connection);
        return element;
    }

    /**
     * @return the jobId
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * get the link to the SAB; note that the build is deleted when topology.keepArtifacts is not set to True.
     * @return the application 
     */
    public String getApplication() {
        return application;
    }

    /**
     * Returns the Streams instance name
     * @return the streamsInstance
     */
    public String getStreamsInstance() {
        return streamsInstance;
    }
}
