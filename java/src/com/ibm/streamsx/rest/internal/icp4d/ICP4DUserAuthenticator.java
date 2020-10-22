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

import java.util.function.Function;

import org.apache.http.client.fluent.Executor;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.internal.RestUtils;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * This class is an Authenticator that authenticates the user by using its user token.
 * Use this interface only with CP4D >= 3.5 and Streams >= 5.5.
 * 
 * @since 1.17
 */
public class ICP4DUserAuthenticator implements Function<Executor,String> {

    private String authHeader;

    /**
     * Creates a new ICP4DUserAuthenticator instance from the service definition JSON object 'topology.service.definition'.
     * CP4D must be at least version 3.5. Requires that the 'topology.service.definition' contains the 'user_token'.
     * @param service the service definition object 'topology.service.definition'
     * @return a new Service instance
     */
    public static ICP4DUserAuthenticator of (JsonObject service, final boolean verify) {
        String userToken = GsonUtilities.jstring (service, StreamsKeys.USER_TOKEN);
        return new ICP4DUserAuthenticator (userToken, verify);
    }

    /**
     * Constructs a new authenticator.
     * @param userToken the user_token from the service definition
     * @param verify SSL connections, to connect to unverified SSL connection, set to false
     */
    private ICP4DUserAuthenticator (String userToken, final boolean verify) {
        super();
        this.authHeader = RestUtils.createBearerAuth (userToken);
    }

    /**
     * Returns an encoded Bearer auth header.
     */
    @Override
    public String apply (Executor t) {
        return this.authHeader;
    }
}
