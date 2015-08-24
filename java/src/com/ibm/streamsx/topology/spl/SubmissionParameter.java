package com.ibm.streamsx.topology.spl;

import com.ibm.streamsx.topology.builder.BSubmissionParameter;

public class SubmissionParameter extends BSubmissionParameter {

    /*
     * A new submission time parameter specification.
     * @param name submission parameter name
     * @param splType SPL type string - e.g., "int32", "uint16"
     */
    public SubmissionParameter(String name, String splType) {
        super(name, splType);
    }

    /**
     * A new submission time parameter specification.
     * @param name submission parameter name
     * @param splType SPL type string - e.g., "int32", "uint16"
     * @param defaultValue default value if parameter isn't specified. may be null.
     */
    public SubmissionParameter(String name, String splType, String defaultValue) {
        super(name, splType, defaultValue);
    }

}
