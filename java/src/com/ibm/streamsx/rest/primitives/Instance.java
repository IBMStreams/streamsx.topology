package com.ibm.streamsx.rest.primitives;

import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.ActiveVersion;

public class Instance {
	private final StreamsConnection connection;
	private final Gson gson = new Gson();
	private InstanceGson instance;

	public Instance(StreamsConnection sc, String gsonInstance) {
		this.connection = sc;
		this.instance = gson.fromJson(gsonInstance, InstanceGson.class);
	};

	public List<Job> getJobs() {
		String sGetJobsURI = instance.jobs;

		String sReturn = "";
		try {
			sReturn = connection.getResponseString(sGetJobsURI);
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<Job> jobs = new JobsArray(connection, sReturn).getJobs();

		return jobs;
	}

	private static class InstanceGson {
		public InstanceGson() {
		};

		public String activeServices;
		public ActiveVersion activeVersion; // not a string
		public String activeViews;
		public String configuredViews;
		public long creationTime;
		public String creationUser;
		public String domain;
		public String exportedStreams;
		public String health;
		public String hosts;
		public String id;
		public String importedStreams;
		public String jobs;
		public String operatorConnections;
		public String operators;
		public String owner;
		public String peConnections;
		public String pes;
		public String resourceAllocations;
		public String resourceType;
		public String restid;
		public String self;
		public long startTime;
		public String startedBy;
		public String status;
		public String views;
	};

	public String getActiveServices() {
		return instance.activeServices;
	}

	public ActiveVersion getActiveVersion() {
		return instance.activeVersion;
	}

	public String getActiveViews() {
		return instance.activeViews;
	}

	public String getConfiguredViews() {
		return instance.configuredViews;
	}

	public long getCreationTime() {
		return instance.creationTime;
	}

	public String getCreationUser() {
		return instance.creationUser;
	}

	public String getDomain() {
		return instance.domain;
	}

	public String getExportedStreams() {
		return instance.exportedStreams;
	}

	public String getHealth() {
		return instance.health;
	}

	public String getHosts() {
		return instance.hosts;
	}

	public String getId() {
		return instance.id;
	}

	public String getImportedStreams() {
		return instance.importedStreams;
	}

	public String getOperatorConnections() {
		return instance.operatorConnections;
	}

	public String getOperators() {
		return instance.operators;
	}

	public String getOwner() {
		return instance.owner;
	}

	public String getPeConnections() {
		return instance.peConnections;
	}

	public String getPes() {
		return instance.pes;
	}

	public String getResourceAllocations() {
		return instance.resourceAllocations;
	}

	public String getResourceType() {
		return instance.resourceType;
	}

	public String getRestid() {
		return instance.restid;
	}

	public String getSelf() {
		return instance.self;
	}

	public long getStartTime() {
		return instance.startTime;
	}

	public String getStartedBy() {
		return instance.startedBy;
	}

	public String getStatus() {
		return instance.status;
	}

	public String getViews() {
		return instance.views;
	}
}
