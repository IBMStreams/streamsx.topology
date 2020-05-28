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
import java.io.InputStream;

/**
 * This class implements a handler that consumes the input stream of a streaming REST response.
 */
public interface InputStreamConsumer<T> {
    /**
     * consumes the input stream. Implementations are not expected to close the stream after consumption.
     * @param is The input stream to consume.
     * @throws IOException
     */
    public void consume(InputStream is) throws IOException;
    
    /**
     * Gets the Object created after consuming the InputStream
     * @return the Object created after consuming the InputStream
     */
    public T getResult(); 
}
