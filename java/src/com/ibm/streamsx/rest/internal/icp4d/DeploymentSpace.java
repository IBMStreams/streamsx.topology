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
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import com.ibm.streamsx.rest.internal.AbstractConnection;

/**
 * This class represents a Deployment space REST resource from the /v2/spaces API
 */
public class DeploymentSpace extends Element {


    /**
     * Create a list as result of a REST query
     * @param con the AbstractConnection implementation 
     * @param url the query URL with potential query parameters
     * @return A list of Objects or an empty list if the query did not return any elements 
     * @throws IOException
     */
    final static List<DeploymentSpace> createSpaceList (AbstractConnection con, String url) throws IOException {
        return createList(con, url, DeploymentSpaceArray.class);
    }


    /**
     * create an object from a JSON response object
     * @param connection
     * @param gsonString
     * @return
     */
    static final DeploymentSpace create (AbstractConnection connection, JsonObject gsonString) {
        DeploymentSpace element = gson.fromJson (gsonString, DeploymentSpace.class);
        element.setConnection (connection);
        return element;
    }


    @Expose
    private Entity entity;

    @Expose
    private MetaData metadata;

    /**
     * @return the entity
     */
    public Entity getEntity() {
        return entity;
    }

    /**
     * @return the metadata
     */
    public MetaData getMetadata() {
        return metadata;
    }

    /**
     * Convenience method for <tt>getMetadata().getId()</tt>.
     * returns the space ID.
     */
    public String getId() {
        return getMetadata().getId();
    }

    /**
     * Convenience method for <tt>getEntity().getName()</tt>.
     * returns the space ID.
     */
    public String getName() {
        return getEntity().getName();
    }

    /**
     * Convenience method for <tt>getMetadata().getUrl()</tt>.
     * returns the path of the space resource URI.
     */
    public String getUriPath() {
        return getMetadata().getUrl();
    }


    /**
     * internal usage to get list of spaces
     */
    private static class DeploymentSpaceArray extends ElementArray<DeploymentSpace> {
        @Expose
        private ArrayList<DeploymentSpace> resources;

        @Override
        List<DeploymentSpace> elements() { return resources; }
    }

    /**
     * entity substructure
     */
    public static class Entity extends Element {
        // expose only some fields
        @Expose
        private String name;  // space name

        @Expose
        private String description;

        /**
         * @return the space name
         */
        public String getName() {
            return name;
        }

        /**
         * @return the space description
         */
        public String getDescription() {
            return description;
        }
    }

    /**
     * metadata substructure
     */
    public static class MetaData extends Element {
        // expose only some fields
        @Expose
        private String id;  // space ID, something like "7e47ee85-777b-48c2-a5f5-813591b69543"

        @Expose
        private String url; // path of the resource URL, for example "/v2/spaces/7e47ee85-777b-48c2-a5f5-813591b69543"

        /**
         * @return the space ID
         */
        public String getId() {
            return id;
        }

        /**
         * @return the Path of the URL
         */
        public String getUrl() {
            return url;
        }
    }
}
