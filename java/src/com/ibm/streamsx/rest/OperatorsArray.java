/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

/**
 * Package class to hold all information about operators from the GET operators URL
 */
class OperatorsArray {
    private List<Operator> operators;
    private OperatorsArrayGson operatorsArray;

    public OperatorsArray(StreamsConnection sc, String gsonOperators) {
        operatorsArray = new Gson().fromJson(gsonOperators, OperatorsArrayGson.class);

        operators = new ArrayList<Operator>(operatorsArray.operators.size());
        for (OperatorGson og : operatorsArray.operators) {
            operators.add(new Operator(sc, og));
        }
    };

    /**
     * @return List of {@Operator}
     */
    public List<Operator> getOperators() {
        return operators;
    }

    private static class OperatorsArrayGson {
        private ArrayList<OperatorGson> operators;
        private String resourceType;
        private int total;
    }

    public String getResourceType() {
        return operatorsArray.resourceType;
    }

    public int getTotal() {
        return operatorsArray.total;
    }

}
