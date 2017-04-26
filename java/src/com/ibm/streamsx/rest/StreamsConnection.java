
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.primitives.Instance;
import com.ibm.streamsx.rest.primitives.InstancesArray;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.Metric;
import com.ibm.streamsx.rest.primitives.Operator;

public class StreamsConnection {

	private String url;
	private String instanceId;
	private String apiKey;

	private Executor executor;

	private final Gson gson = new Gson();

	private void setURL(String url) {
		if (url.equals("") || (url.charAt(url.length() - 1) != '/')) {
			this.url = url;
		} else {
			this.url = url.substring(0, url.length() - 1);
		}
	}

	public StreamsConnection(String userName, String authToken, String url) {
		String apiCredentials = userName + ":" + authToken;
		this.apiKey = "Basic " + DatatypeConverter.printBase64Binary(apiCredentials.getBytes(StandardCharsets.UTF_8));

		this.executor = Executor.newInstance();
		this.setURL(url);
	}

	public String setStreamsInstanceRestURL(String url) {
		String old_url = this.url;
		this.setURL(url);
		return old_url;
	}

	public String setInstanceId(String id) {
		this.instanceId = id;
		return id;
	}

	public String getResponseString(String inputString) throws ClientProtocolException, IOException {
		String sReturn = "";
		Request request = Request.Get(inputString).addHeader(AUTH.WWW_AUTH_RESP, this.apiKey).useExpectContinue();

		Response response = executor.execute(request);
		HttpResponse hResponse = response.returnResponse();
		int rcResponse = hResponse.getStatusLine().getStatusCode();

		if (HttpStatus.SC_OK == rcResponse) {
			sReturn = EntityUtils.toString(hResponse.getEntity());
		}
		else {
			String httpError = "HttpStatus is " + rcResponse + " for url " + inputString ;
			throw new IllegalStateException(httpError);
		}
		// FIXME: remove these lines
		System.out.println("-----------------");
		System.out.println(inputString);
		System.out.println(rcResponse + ": " + sReturn);
		System.out.println("-----------------");
		return sReturn;
	}

	public List<Instance> getInstances() throws ClientProtocolException, IOException{
		String instancesURL = url + "/instances/";

		String sReturn = getResponseString(instancesURL);
		InstancesArray iArray = new InstancesArray(this, sReturn);

		return iArray.getInstances();
	}

	public Instance getInstance() throws ClientProtocolException, IOException {
		Instance si = null;
		if (this.instanceId.equals("")) {
         // should add some fallback code to see if there's only one instance
			throw new IllegalArgumentException("Missing instance name");
		} else {
			String instanceURL = url + "/instances/" + this.instanceId;
			String sReturn = getResponseString(instanceURL);

			si = new Instance(this, sReturn);
		}
		return si;
	}

	public Instance getInstance(String instanceId) throws ClientProtocolException, IOException{
		this.instanceId = instanceId;
		return this.getInstance();
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

		try {
			System.out.println("Returning instances");
			List<Instance> instances = sClient.getInstances();

			for (Instance instance : instances) {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
