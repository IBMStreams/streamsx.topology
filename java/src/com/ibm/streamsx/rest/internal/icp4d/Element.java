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
import java.util.Collections;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.ibm.streamsx.rest.internal.AbstractConnection;

/**
 * Abstract base class for REST elements returned from the CP4D REST API.
 */
abstract class Element {

    protected static final Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().enableComplexMapKeySerialization().create();

    private static final Gson pretty = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
    private AbstractConnection connection;

    AbstractConnection connection() {
        return connection;
    }

    void setConnection(AbstractConnection connection) {
        this.connection = connection;
    }

    @Override
    public final String toString() {
        return pretty.toJson(this);
    }

    static final <E extends Element> E create (final AbstractConnection sc, String uri, Class<E> elementClass) throws IOException {
        if (uri == null) {
            return null;
        }
        return createFromResponse(sc, sc.getResponseString(uri), elementClass);
    }

    static final <E extends Element> E createFromResponse (final AbstractConnection sc, String response, Class<E> elementClass) throws IOException {
        E element = gson.fromJson(response, elementClass);
        element.setConnection(sc);
        return element;
    }

    static final <E extends Element> E createFromResponse (final AbstractConnection sc, JsonObject response, Class<E> elementClass) throws IOException {
        E element = gson.fromJson(response, elementClass);
        element.setConnection(sc);
        return element;
    }


    /**
     * internal usage to get the list of elements
     */
    protected abstract static class ElementArray<E extends Element> {

        abstract List<E> elements();
    }

    protected final static <E extends Element, A extends ElementArray<E>> List<E> createList (AbstractConnection sc, String uri, Class<A> arrayClass) throws IOException {
        // Assume not supported if no associated URI.
        if (uri == null) {
            return Collections.emptyList();
        }
        try {
            A array = gson.fromJson(sc.getResponseString(uri), arrayClass);
            for (Element e : array.elements()) {
                e.setConnection(sc);
            }
            return array.elements();
        } catch (JsonSyntaxException e) {
            return Collections.emptyList();
        }
    }
}
