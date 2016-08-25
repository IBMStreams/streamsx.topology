/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
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
