
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.auth.AUTH;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.primitives.Instance;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.Metric;
import com.ibm.streamsx.rest.primitives.Operator;

public class StreamsConnection {

	private String url;
	private String apiKey;

	private Executor executor;

	private final Gson gson = new Gson();

	public StreamsConnection(String userName, String authToken, String url) {
		try {
			URL xUrl = new URL(url);
			String hostName = xUrl.getHost();

			String apiCredentials = userName + ":" + authToken;
			this.apiKey = "Basic "
					+ DatatypeConverter.printBase64Binary(apiCredentials.getBytes(StandardCharsets.UTF_8));

			this.executor = Executor.newInstance();

			this.url = url;
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	public String getResponseString(String inputString) throws ClientProtocolException, IOException {
		String sReturn = "";
		try {
			Request request = Request.Get(inputString).addHeader(AUTH.WWW_AUTH_RESP, this.apiKey).useExpectContinue();

			Response response = executor.execute(request);

			// TODO: need to decode errors
			sReturn = response.returnContent().asString();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		}
		return sReturn;
	}

	public Instance getInstance(String instanceName) {
		StringBuilder sb = new StringBuilder();
		sb.append(url);
		sb.append("instances/" + instanceName);

		String sReturn = "";
		try {
			sReturn = getResponseString(sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// need to check return code
		System.out.println(sReturn);
		Instance si = new Instance(this, sReturn);

		return si;
	}

	public static void main(String[] args) {
		String userName = args[0];
		String authToken = args[1];
		String url = args[2];
		String instanceName = args[3];

		System.out.println(userName);
		System.out.println(authToken);
		System.out.println(url);
		System.out.println(instanceName);
		StreamsConnection sClient = new StreamsConnection(userName, authToken, url);

		System.out.println("Returning instance");
		Instance instance = sClient.getInstance(instanceName);

		System.out.println("Returning jobs");
		List<Job> jobs = instance.getJobs();

		System.out.println("Returning operators");
		for (Job job : jobs) {
			System.out.println("Looking at job");
			List<Operator> operators = job.getOperators();
			for (Operator op : operators) {
				System.out.println("Looking at metrics for job");
				List<Metric> metrics = op.getMetrics();
			}
		}
	}
}
