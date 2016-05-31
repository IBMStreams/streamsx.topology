package com.ibm.streamsx.topology.builder;

/**
 * A parameter to an operator is represented by
 * a type and a value. HereThis contains constants
 * representing the special types.
 *
 */
public interface JParamTypes {
	/**
	 * An SPL enum value as a string.
	 */
	String TYPE_ENUM = "enum";
	
	/**
	 * An SPL schema type as a string.
	 */
	String TYPE_SPLTYPE = "spltype";
	
	/**
	 * An SPL input attribute as a string.
	 */
	String TYPE_ATTRIBUTE = "attribute";
	
	 /** operator parameter type for submission parameter value */
    String TYPE_SUBMISSION_PARAMETER = "submissionParameter";
    
    /**
     * An SPL expression.
     */
    String TYPE_SPL_EXPRESSION = "splexpr";
}
