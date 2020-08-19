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
package com.ibm.streamsx.rest.build;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * This class describes a build pool
 * URL: /streams/rest/buildpools
 */
public class BuildPool extends Element {

    /**
     * internal usage to get list of images and toolkits
     */
    private static class BuildPoolArray extends ElementArray<BuildPool> {
        @Expose
        private ArrayList<BuildPool> buildPools;

        @Override
        List<BuildPool> elements() { return buildPools; }
    }

    @Expose
    private String name;
    @Expose
    private String restid;
    @Expose
    private String type;
    @Expose 
    private String toolkits;
    
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    /**
     * @return the restid
     */
    public String getRestid() {
        return restid;
    }
    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @return the toolkits URL. This field is only available for build pools of type 'application'
     */
    public String getToolkits() {
        return toolkits;
    }
    
    /* more fields:
    buildInactivityTimeout  15
    buildProcessingTimeout  15
    buildProcessingTimeoutMaximum   15
    buildProductVersion ""
    buildingCount   0
    resourceType    "buildPool"
    resourceWaitTimeout 2
    sizeMaximum 5
    sizeMinimum 1
    status  "ready"
    waitingCount    0
     */
    final static List<BuildPool> createPoolList(AbstractConnection sc, String uri) throws IOException {
        return createList(sc, uri, BuildPoolArray.class);
    }

    final static List<BuildPool> createPoolList(AbstractConnection sc, String uri, String type) throws IOException {
        List<BuildPool> pools = createPoolList(sc, uri);
        int cnt = 0;
        for (BuildPool p: pools) {
            if (p.getType().equals(type)) {
                ++cnt;
            }
        }
        if (cnt == 0) return Collections.emptyList();
        ArrayList<BuildPool> res = new ArrayList<>(cnt);
        for (BuildPool p: pools) {
            if (p.getType().equals(type)) {
                res.add(p);
            }
        }
        return res;
    }
}
