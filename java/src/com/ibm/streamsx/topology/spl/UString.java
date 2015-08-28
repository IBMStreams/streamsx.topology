package com.ibm.streamsx.topology.spl;

/**
 * A wrapper class for specifying SPL "ustring" values.
 * <p>
 * Use:
 * <pre>{@code
 * // to specify an SPL operator parameter value
 * String str = ...;
 * Map<String,Object> params = ...
 * params.put("aUstringOpParam", new UString(str));
 * params.put("aUstringOpParam", topology.getSubmissionParameter(..., new UString(str));
 * params.put("aUstringOpParam", topology.getSubmissionParameter(..., UString.class);
 * ... = SPL.invokeOperator(..., params);
 * 
 * // to specify an SPL ustring parameter's submission time value
 * Map<String,Object> config = new HashMap<>();
 * Map<String,Object> submitParams = new HashMap<>();
 * submitParams.put("paramName", new UString(...);
 * config.put(ContextProperties.SUBMISSION_PARAMS, submitParams);
 * StreamsContextFactory.getStreamsContext(DISTRIBUTED)
 *     .submit(topology); 
 * }</pre>
 */
public class UString {
    private final String value;
    /**
     * Create a SPL ustring wrapper
     * @param value a string
     * @throws IllegalArgumentException if {@code value} is null
     */
    public UString(String value) {
        if (value == null)
            throw new IllegalArgumentException("value");
        this.value = value;
    }
    public String value() {
        return value;
    }
    @Override
    public String toString() {
        return value.toString();
    }
}
