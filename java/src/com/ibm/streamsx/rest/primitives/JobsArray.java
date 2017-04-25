package com.ibm.streamsx.rest.primitives;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.JobGson;

public class JobsArray {
	private final StreamsConnection connection;
	private final Gson gson = new Gson();
	private List<Job> jobs;
	private JobsArrayGson jobArray;

	public JobsArray(StreamsConnection sc, String gsonJobs) {
		this.connection = sc;
		this.jobArray = gson.fromJson(gsonJobs, JobsArrayGson.class);

		this.jobArray.jobsList = new ArrayList<Job>(jobArray.jobs.size());
		for (JobGson jg : jobArray.jobs) {
			jobArray.jobsList.add(new Job(sc, jg));
		}
		this.jobs = jobArray.jobsList;
	};

	public List<Job> getJobs() {
		return jobs;
	}

	private static class JobsArrayGson {
		public ArrayList<JobGson> jobs;
		public ArrayList<Job> jobsList;
		public String resourceType;
		public int total;
	}

	public String getResourceType() {
		return jobArray.resourceType;
	}

	public int getTotal() {
		return jobArray.total;
	}

}
