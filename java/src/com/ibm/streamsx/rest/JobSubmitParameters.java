/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;

/**
 * Holds Submisson Paramters for a Job
 */
public class JobSubmitParameters {

    /**
     * @return the composite
     */
    public String getComposite() {
        return composite;
    }

    /**
     * @return the defaultValue
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the parameterKind from the following list:
     *         <ul>
     *         <li>named</li>
     *         <li>namedList</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getParameterKind() {
        return parameterKind;
    }

    /**
     * @return the required
     */
    public boolean isRequired() {
        return required;
    }

    /**
     * @return the value, if parameterKind is a namedList, this will be a comma
     *         separated list
     *
     */
    public String getValue() {
        return value;
    }

    /**
     * @return the values for the submission parameter. If parameterKind is
     *         named, this is a single entry list
     */
    public ArrayList<String> getValues() {
        return values;
    }

    @Expose
    private String composite;
    @Expose
    private String defaultValue;
    @Expose
    private String name;
    @Expose
    private String parameterKind;
    @Expose
    private boolean required;
    @Expose
    private String value;
    @Expose
    private ArrayList<String> values;
}
