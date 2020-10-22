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
package com.ibm.streamsx.rest.internal;

import java.io.IOException;

import org.apache.http.client.fluent.Executor;

/**
 * This class represents an abstract connection.
 */
public abstract class AbstractConnection {

    private Executor executor;

    /**
     * Returns the Authorization header without the <tt>Authorization:</tt> keyword.
     * Subclasses must implement this method. 
     * @return the the Authorization header value.
     */
    public abstract String getAuthorization();

    /**
     * Connection to a REST API
     * 
     * @param allowInsecure
     *            Flag to allow insecure TLS/SSL connections. This is
     *            <strong>not</strong> recommended in a production environment
     */
    public AbstractConnection(boolean allowInsecure) {
        this.executor = RestUtils.createExecutor(allowInsecure);
    }

    public boolean allowInsecureHosts(boolean allowInsecure) {
        this.executor = RestUtils.createExecutor(allowInsecure);
        return allowInsecure;
    }

    /**
     * access to the Executor.
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Gets a response to an HTTP call
     * 
     * @param inputString
     *            REST call to make
     * @return response from the inputString
     * @throws IOException
     */
    public String getResponseString(String url) throws IOException {
        return RestUtils.getResponseString(executor, getAuthorization(), url);
    }
}
