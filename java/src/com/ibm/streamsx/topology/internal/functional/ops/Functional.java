/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

public interface Functional {
    /**
     * Gets the functional logic string
     * @return string containing functional logic
     */
    public String getFunctionalLogic();

    /**
     * Sets the functional logic string
     * @param logic A string containing serialized functional logic
     */
    public void setFunctionalLogic(String logic);

    /**
     * Gets the list of jars that will be loaded by the operator
     * @return A list of strings for the jars that will be loaded by the
     * operator
     */
    public String[] getJar();

    /**
     * Sets the list of jars that will be loaded by the operator upon initialization.
     * @param jar A list of strings of the names of the jars that should be 
     * loaded upon initialization.
     */
    public void setJar(String[] jar);
}
