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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import com.ibm.streamsx.rest.internal.AbstractConnection;

/**
 * This class represents a Job REST resource from the /v2/jobs API of the CP4D.
 * 
 * @since 1.17
 */
public class JobDescription extends Element {

    /**
     * Streams jobs must have this asset refernce type.
     */
    public final static String STREAMS_ASSET_REF_TYPE = "streams";

    @Expose
    private Entity entity;

    @Expose
    private MetaData metadata;

    /**
     * Create a list as result of a REST query
     * @param con the AbstractConnection implementation 
     * @param url the query URL with potential query parameters
     * @param jobName a job name that must fully match the name returned by {@link #getName()}. If null is specified, all jobs are returned.
     * @return A list of Objects or an empty list if the query did not return any elements 
     * @throws IOException
     */
    final static List<JobDescription> createJobList (AbstractConnection con, String url, String jobName) throws IOException {
        List<JobDescription> jobList = createList(con, url, JobArray.class);
        ArrayList<JobDescription> filtered = new ArrayList<>(jobList.size());
        final boolean filterForName = jobName != null;
        for (JobDescription job: jobList) {
            if (!job.isStreamsJob()) continue;
            if (filterForName) {
                if (jobName.equals (job.getName())) {
                    filtered.add(job);
                }
            }
            else {
                filtered.add(job);
            }
        }
        return filtered;
    }

    /**
     * @return the entity
     */
    public Entity getEntity() {
        return entity;
    }

    /**
     * @return the metadata
     */
    public MetaData getMetaData() {
        return metadata;
    }

    /**
     * returns true, if the asset_ref_type of the entity is "streams", i.e. if this is a Streams job.
     * @return true, when asset_ref_type == "streams", false otherwise
     */
    public boolean isStreamsJob() {
        return JobDescription.STREAMS_ASSET_REF_TYPE.equals(this.entity.getJob().getAssetRefType());
    }


    /**
     * Returns the name of the job.
     * Convenience method for getMetadata().getName()
     * @return the name of the job
     */
    public String getName() {
        return getMetaData().getName();
    }

    /**
     * Returns the asset_id of the job, something like "19561431-e6bd-4bfa-8f71-630821ed4e14"
     * Convenience method for getMetadata().getAsset_id();
     * @return the asset_id
     */
    public String getAssetId() {
        return getMetaData().getAsset_id();
    }

    // ----- inner classes for sub-structures ----
    /**
     * internal usage to get list of images and toolkits
     */
    private static class JobArray extends ElementArray<JobDescription> {
        @Expose
        private ArrayList<JobDescription> results;

        @Override
        List<JobDescription> elements() { return results; }
    }

    /**
     * metadata sub-structure
     *
     *  "metadata": {
     *     "asset_id": "19561431-e6bd-4bfa-8f71-630821ed4e14",
     *     "description": "jobs for test",
     *     "name": "python job",
     *     "owner_id": "1000330999",
     *     "version": 0
     * }
     * 
     */
    public static class MetaData extends Element {
        @Expose
        private String asset_id;

        @Expose
        private String project_id;

        @Expose
        private String space_id;

        @Expose
        private String description;

        @Expose
        private String name;

        @Expose
        private String owner_id;

        @Expose
        private int version;

        /**
         * @return the asset_id
         */
        public String getAsset_id() {
            return asset_id;
        }

        /**
         * @param project_id the project_id to set
         */
        public void setProject_id(String project_id) {
            this.project_id = project_id;
        }

        /**
         * @param space_id the space_id to set
         */
        public void setSpace_id(String space_id) {
            this.space_id = space_id;
        }

        /**
         * @return the project_id the job is associated with or null if the job is associated with a deployment space
         */
        public String getProject_id() {
            return project_id;
        }

        /**
         * @return the space_id the job is associated with or null if the job is associated with a project
         */
        public String getSpace_id() {
            return space_id;
        }

        /**
         * @return the description
         */
        public String getDescription() {
            return description;
        }

        /**
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * @return the owner_id
         */
        public String getOwner_id() {
            return owner_id;
        }

        /**
         * @return the version
         */
        public int getVersion() {
            return version;
        }
    }

    /**
     * Returns the query parameter for what the job is associated with, for example "space_id=54b262a0-cf67-4ac9-82bb-69caa8cabcae".
     * The query parameter is encoded to application/x-www-form-urlencoded MIME format. 
     * @return the query parameter for space_id or project_id or null if this object has neither projectId nor spaceId.
     * @throws UnsupportedEncodingException should not be thrown as UTF-8 is always supported
     */
    String createAssociatedWithQueryParam() throws UnsupportedEncodingException {
        final String projectId = getMetaData().getProject_id();
        if (projectId != null) {
            return "project_id=" + URLEncoder.encode (projectId, StandardCharsets.UTF_8.name());
        }
        final String spaceId = getMetaData().getSpace_id();
        if (spaceId != null) {
            return "space_id=" + URLEncoder.encode (spaceId, StandardCharsets.UTF_8.name());
        }
        return null;
    }

    /**
     * create an object from a JSON response object
     * @param connection
     * @param gsonString
     * @return
     */
    static final JobDescription create (AbstractConnection connection, JsonObject gsonString) {
        JobDescription element = gson.fromJson (gsonString, JobDescription.class);
        element.setConnection (connection);
        return element;
    }

    /**
     * entity substructure
     * 
     * "entity": {
     *   "job": {
     *     "asset_ref_type": "streams",
     *     "configuration": {
     *       "application": "https://cluster.ip/v2/asset_files/jobs/19561431-e6bd-4bfa-8f71-630821ed4e14/View.ConflictingAttributes.sab?space_id=66d50216-6048-4619-81cc-a2f38fa70fdb",
     *       "env_type": "streams",
     *       "env_variables": [],
     *       "streamsInstance": "sample-streams"
     *     },
     *     "last_run_initiator": "None",
     *     "last_run_status": "None",
     *     "last_run_status_timestamp": 0,
     *     "last_run_time": "",
     *     "space_name": "sample-streams.nbgr55"
     *   }
     * }
     *
     */
    public class Entity extends Element {
        @Expose
        private _Job job;

        /**
         * @return the job
         */
        public _Job getJob() {
            return job;
        }

        private class _Job extends Element {
            @Expose
            private String asset_ref_type;

            /**
             * @return the asset_ref_type
             */
            public String getAssetRefType() {
                return asset_ref_type;
            }
        }
    }
}
