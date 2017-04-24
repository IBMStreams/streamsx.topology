package com.ibm.streamsx.rest.primitives;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;

public class MetricsArray {
	private final StreamsConnection connection;
	private final Gson gson = new Gson();
	private List<Metric> metrics;
	private MetricsArrayGson metricsArray;

	public MetricsArray(StreamsConnection sc, String gsonMetrics) {
		this.connection = sc;
		this.metricsArray = gson.fromJson(gsonMetrics, MetricsArrayGson.class);

		this.metricsArray.metricsList = new ArrayList<Metric>(metricsArray.metrics.size());
		for (MetricGson mg : metricsArray.metrics) {
			metricsArray.metricsList.add(new Metric(sc, mg));
		}
		this.metrics = metricsArray.metricsList;
	};

	public List<Metric> getMetrics() {
		return metrics;
	}

	private static class MetricsArrayGson {
		public ArrayList<MetricGson> metrics;
		public ArrayList<Metric> metricsList;
		public String owner;
		public String resourceType;
		public long total;
	}

	public String getOwner() {
		return metricsArray.owner;
	}

	public String getResourceType() {
		return metricsArray.resourceType;
	}

	public long getTotal() {
		return metricsArray.total;
	}

}
