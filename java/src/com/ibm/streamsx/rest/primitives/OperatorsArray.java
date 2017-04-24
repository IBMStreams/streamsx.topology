package com.ibm.streamsx.rest.primitives;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;

public class OperatorsArray {
	private final StreamsConnection connection;
	private final Gson gson = new Gson();
	private List<Operator> operators;
	private OperatorsArrayGson operatorsArray;

	public OperatorsArray(StreamsConnection sc, String gsonOperators) {
		this.connection = sc;
		this.operatorsArray = gson.fromJson(gsonOperators, OperatorsArrayGson.class);

		this.operatorsArray.operatorsList = new ArrayList<Operator>(operatorsArray.operators.size());
		for (OperatorGson og : operatorsArray.operators) {
			operatorsArray.operatorsList.add(new Operator(sc, og));
		}
		this.operators = operatorsArray.operatorsList;
	};

	public List<Operator> getOperators() {
		return operators;
	}

	private static class OperatorsArrayGson {
		private ArrayList<OperatorGson> operators;
		private ArrayList<Operator> operatorsList;
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
