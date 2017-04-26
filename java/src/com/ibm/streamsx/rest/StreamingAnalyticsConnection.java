
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.rest.primitives.Instance;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.Metric;
import com.ibm.streamsx.rest.primitives.Operator;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class StreamingAnalyticsConnection {

	private StreamsConnection connection;
	private String streaming_analytics_rest_url;
	private String instance_rest_url;
	private Instance instance;

	public StreamingAnalyticsConnection(String credentialsFile, String serviceName) throws ClientProtocolException, IOException {

		JsonObject SAcredentials = new JsonObject();

		SAcredentials.addProperty(SERVICE_NAME, serviceName);
		SAcredentials.addProperty(VCAP_SERVICES, credentialsFile);

		JsonObject service = VcapServices.getVCAPService(SAcredentials);

		JsonObject credential = new JsonObject();
		credential = service.get("credentials").getAsJsonObject();

		String userId = credential.get("userid").getAsString();
		String authToken = credential.get("password").getAsString();
		String resourcesPath = credential.get("resources_path").getAsString();
		String sURL = credential.get("rest_url").getAsString() + resourcesPath;

		connection = new StreamsConnection(userId, authToken, "");

		String sResources = connection.getResponseString(sURL);

		if (!sResources.equals("")) {
			JsonParser jParse = new JsonParser();
			JsonObject resources = jParse.parse(sResources).getAsJsonObject();

			String restURL = resources.get("streams_rest_url").getAsString();
			connection.setStreamsInstanceRestURL(restURL);
		} else {
			throw new IllegalStateException("Missing restURL for service");
		}

		String[] rTokens = resourcesPath.split("/");
		if (rTokens[3].equals("service_instances")) {
			connection.setInstanceId(rTokens[4]);
		} else {
			throw new IllegalStateException("Resource Path decoding error.");
		}
	}

	public Instance getInstance() throws ClientProtocolException, IOException {
		return connection.getInstance();
	}

	public Instance getInstance(String instanceId) throws ClientProtocolException, IOException {
		return connection.getInstance(instanceId);
	}

	public static void main(String[] args) {
		String credentials = args[0];
		String serviceName = args[1];

		System.out.println(credentials);
		System.out.println(serviceName);

		try {
			StreamingAnalyticsConnection sClient = new StreamingAnalyticsConnection(credentials, serviceName);

			System.out.println("Returning instance");
			Instance instance = sClient.getInstance();

			System.out.println("Returning jobs");
			List<Job> jobs = instance.getJobs();
			for (Job job : jobs) {
				System.out.println("Looking at job: " + job.getId());
				List<Operator> operators = job.getOperators();
				for (Operator op : operators) {
					System.out.println("Looking at metrics for job");
					List<Metric> metrics = op.getMetrics();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
