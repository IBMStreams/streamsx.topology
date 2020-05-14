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
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * This class describes an image from the build pool.
 * URL: /streams/rest/buildpools/<i>pool-ID</i>/baseimages
 */
public class BaseImage extends Element {

    /**
     * internal usage to get list of images
     */
    private static class ImagesArray extends ElementArray<BaseImage> {
        @Expose
        private ArrayList<BaseImage> images;

        @Override
        List<BaseImage> elements() { return images; }
    }

    @Expose
    private String registry;
    @Expose
    private String prefix;
    @Expose
    private String name;
    @Expose
    private String tag;
    @Expose
    private String id;
    @Expose
    private String restid;


    /**
     * @return the registry
     */
    public String getRegistry() {
        return registry;
    }


    /**
     * @return the prefix
     */
    public String getPrefix() {
        return prefix;
    }


    /**
     * @return the name
     */
    public String getName() {
        return name;
    }


    /**
     * @return the tag
     */
    public String getTag() {
        return tag;
    }


    /**
     * @return the id
     */
    public String getId() {
        return id;
    }


    /**
     * @return the restid
     */
    public String getRestid() {
        return restid;
    }

    private BaseImage() { }

    final static List<BaseImage> createImageList(AbstractConnection sc, String uri) throws IOException {
        return createList(sc, uri, ImagesArray.class);
    }
}
