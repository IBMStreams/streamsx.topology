package com.ibm.streamsx.rest.primitives;

import java.io.IOException;
import java.util.List;
import org.apache.http.client.ClientProtocolException;

import com.google.gson.Gson;

import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.InstanceGson;
import com.ibm.streamsx.rest.primitives.ActiveVersion;

public class Instance {
	private final StreamsConnection connection;
	private final Gson gson = new Gson();
	private InstanceGson instance;

	public Instance(StreamsConnection sc, String gsonInstance) {
		this.connection = sc;
		this.instance = gson.fromJson(gsonInstance, InstanceGson.class);
	};

	public Instance(StreamsConnection sc, InstanceGson gsonInstance) {
		this.connection = sc;
		this.instance = gsonInstance;
	};

	public List<Job> getJobs() throws ClientProtocolException, IOException {
		String sGetJobsURI = instance.jobs;
		String sReturn = connection.getResponseString(sGetJobsURI);

		List<Job> jobs = new JobsArray(connection, sReturn).getJobs();
		return jobs;
	}

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
