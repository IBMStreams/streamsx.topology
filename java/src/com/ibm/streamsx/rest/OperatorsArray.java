/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * Package class to hold all information about operators from the GET operators URL
 */
class OperatorsArray {
    private OperatorsArrayGson operatorsArray;

    public OperatorsArray(StreamsConnection sc, String gsonOperators) {
        operatorsArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                                          .fromJson(gsonOperators, OperatorsArrayGson.class);

        for (Operator op : operatorsArray.operators) {
            op.setConnection(sc);
        }
    };

    /**
     * @return List of {@Operator}
     */
    public List<Operator> getOperators() {
        return operatorsArray.operators;
    }

    private static class OperatorsArrayGson {
        @Expose
        private ArrayList<Operator> operators;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

    public String getResourceType() {
        return operatorsArray.resourceType;
    }

    public int getTotal() {
        return operatorsArray.total;
    }

}
