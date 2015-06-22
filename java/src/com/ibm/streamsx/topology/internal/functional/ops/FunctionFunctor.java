/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.topology.internal.functional.FunctionalHelper;

/**
 * 
 * Common code for operators with inputs and outputs.
 * 
 */
@SharedLoader
public abstract class FunctionFunctor extends AbstractOperator implements Functional{

    public static final String FUNCTIONAL_LOGIC_PARAM = "functionalLogic";

    // parameters
    private String functionalLogic;
    private String[] jar;

    public final String getFunctionalLogic() {
        return functionalLogic;
    }

    @Parameter
    public void setFunctionalLogic(String logic) {
        this.functionalLogic = logic;
    }

    public final String[] getJar() {
        return jar;
    }

    @Parameter(optional = true)
    public final void setJar(String[] jar) {
        this.jar = jar;
    }

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        FunctionalHelper.addLibraries(this, getJar());
    }
}
