/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional;

import java.io.File;
import java.net.MalformedURLException;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.internal.spljava.Schemas;

public class FunctionalHelper {

    @SuppressWarnings("unchecked")
    public static <T> SPLMapping<T> getInputMapping(AbstractOperator operator,
            int port) {
        return (SPLMapping<T>) Schemas.getSPLMapping(operator.getInput(port)
                .getStreamSchema());
    }

    @SuppressWarnings("unchecked")
    public static <T> SPLMapping<T> getOutputMapping(AbstractOperator operator,
            int port) {
        return (SPLMapping<T>) Schemas.getSPLMapping(operator.getOutput(port)
                .getStreamSchema());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getLogicObject(String logicString)
            throws ClassNotFoundException {
        return (T) ObjectUtils.deserializeLogic(logicString);
    }

    public static void addLibraries(AbstractOperator operator,
            String[] libraries) throws MalformedURLException {
        if (libraries == null)
            return;
        File appDir = operator.getOperatorContext().getPE()
                .getApplicationDirectory();
        String[] urls = new String[libraries.length];
        for (int i = 0; i < libraries.length; i++) {
            File f = new File(appDir, libraries[i]);
            urls[i] = f.toURI().toURL().toString();
        }

        operator.getOperatorContext().addClassLibraries(urls);

    }
}
