package com.ibm.streamsx.rest.internal;

/**
 * Build types for the build service.
 * Use {@link #getJsonValue()} to create the value for the JSON of the request body.
 */
public enum BuildType {
    APPLICATION ("application"),
    STREAMS_DOCKER_IMAGE ("streamsDockerImage");

    private final String jsonValue;
    BuildType (final String jsonValue) {
        this.jsonValue = jsonValue;
    }

    /**
     * @return the value as it must go into the JSON of the request body.
     */
    public String getJsonValue() {
        return jsonValue;
    }
}
