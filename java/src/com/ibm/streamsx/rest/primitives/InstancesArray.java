package com.ibm.streamsx.rest.primitives;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.primitives.Instance;
import com.ibm.streamsx.rest.primitives.InstanceGson;

public class InstancesArray {
	private final StreamsConnection connection;
	private final Gson gson = new Gson();
	private List<Instance> instances;
	private InstancesArrayGson instanceArray;

	public InstancesArray(StreamsConnection sc, String gsonInstances) {
		this.connection = sc;
		this.instanceArray = gson.fromJson(gsonInstances, InstancesArrayGson.class);

		this.instanceArray.instancesList = new ArrayList<Instance>(instanceArray.instances.size());
		for (InstanceGson ig : instanceArray.instances) {
			instanceArray.instancesList.add(new Instance(sc, ig));
		}
		this.instances = instanceArray.instancesList;
	};

	public List<Instance> getInstances() {
		return instances;
	}

	private static class InstancesArrayGson {
		public ArrayList<InstanceGson> instances;
		public ArrayList<Instance> instancesList;
		public String resourceType;
		public int total;
	}

	public String getResourceType() {
		return instanceArray.resourceType;
	}

	public int getTotal() {
		return instanceArray.total;
	}

}
