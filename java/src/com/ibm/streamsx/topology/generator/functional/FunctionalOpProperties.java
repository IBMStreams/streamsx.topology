/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
 */
package com.ibm.streamsx.topology.generator.functional;

public interface FunctionalOpProperties {
    String FUNCTIONAL_LOGIC_PARAM = "functionalLogic";
    
    /** The name of the functional operator's actual SPL parameter
     * for the submission parameters names */
    String NAME_SUBMISSION_PARAM_NAMES = "submissionParamNames";
    /** The name of the functional operator's actual SPL parameter
     * for the submission parameters values */
    String NAME_SUBMISSION_PARAM_VALUES = "submissionParamValues";

}
