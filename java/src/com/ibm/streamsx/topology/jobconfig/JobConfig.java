/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.jobconfig;

import static com.ibm.streamsx.topology.context.ContextProperties.SUBMISSION_PARAMS;
import static com.ibm.streamsx.topology.context.ContextProperties.TRACING_LEVEL;
import static com.ibm.streamsx.topology.context.JobProperties.CONFIG;
import static com.ibm.streamsx.topology.context.JobProperties.DATA_DIRECTORY;
import static com.ibm.streamsx.topology.context.JobProperties.GROUP;
import static com.ibm.streamsx.topology.context.JobProperties.NAME;
import static com.ibm.streamsx.topology.context.JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION;
import static com.ibm.streamsx.topology.context.JobProperties.PRELOAD_APPLICATION_BUNDLES;
import static com.ibm.streamsx.topology.internal.streams.Util.getConfigEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.internal.gson.JSON4JBridge;
import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * A job configuration.
 * 
 * Used to control the submission of a job to a distributed context.
 *
 */
public class JobConfig {

    private String jobName;
    private String jobGroup;
    private String dataDirectory;
    private transient Boolean overrideResourceLoadProtection;
    private Boolean preloadApplicationBundles;
    private List<SubmissionParameter> submissionParameters;
    private transient Level _tracing;
    private String tracing;

    /**
     * An empty job configuration.
     */
    public JobConfig() {
    }

    /**
     * Job configuration with a job name and group.
     * @param jobName Job group, can be {@code null}.
     * @param jobGroup Job name, can be {@code null}.
     */
    public JobConfig(String jobGroup, String jobName) {
		setJobGroup(jobGroup);
		setJobName(jobName);
	}
    
    public JobConfig addToConfig(Map<String, Object> config) {
        config.put(JobProperties.CONFIG, this);
        return this;
    }

    /**
     * Get the job name.
     * @return Job name, {@code null} if it is not set.
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Set the job name.
     * @param jobName Job name, {@code null} unsets the name.
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * Get the job group.
     * @return Job group, {@code null} if it is not set.
     */
    public String getJobGroup() {
        return jobGroup;
    }

    /**
     * Set the job group.
     * @param jobGroup Job group, {@code null} unsets the group.
     */
    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }
    
    /**
     * Get the data directory.
     * @return Data directory, {@code null} if it is not set.
     */
    public String getDataDirectory() {
        return dataDirectory;
    }

    /**
     * Set the data directory.
     * The data directory must be a valid path on the
     * IBM Streams instance the job will execute on.
     * @param dataDirectory Data directory, {@code null} unsets the data directory.
     */
    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }
    
    public Boolean getOverrideResourceLoadProtection() {
        return overrideResourceLoadProtection;
    }

    public void setOverrideResourceLoadProtection(Boolean overrideResourceLoadProtection) {
        this.overrideResourceLoadProtection = overrideResourceLoadProtection;
    }

    /**
     * @return the tracing
     */
    public Level getTracing() {
        return _tracing;
    }
    
    public String getStreamsTracing() {
        return tracing;
    }

    /**
     * @param tracing the tracing to set
     */
    public void setTracing(Level tracing) {
        if (tracing == null)
            this.tracing = null;
        else
            this.tracing = toTracingLevel(tracing);
        this._tracing = tracing;
    }

    public Boolean getPreloadApplicationBundles() {
        return preloadApplicationBundles;
    }

    public void setPreloadApplicationBundles(Boolean preloadApplicationBundles) {
        this.preloadApplicationBundles = preloadApplicationBundles;
    }

    /**
     * Have any submission parameters been added.
     * @return {@code true} if at least one submission exists, otherwise {@code false}.
     */
    public boolean hasSubmissionParameters() {
        return  submissionParameters != null
                && !submissionParameters.isEmpty();
    }
    
    private List<SubmissionParameter> createSubmissionParameters() {
        if (submissionParameters == null)
            submissionParameters = new ArrayList<>();
        return submissionParameters;
    }
    
    /**
     * Get the submission parameters.
     * Any modifications to the returned list modify the submission
     * parameters for this object.
     * @return Submission parameters, will be empty if none have been set.
     */
    public List<SubmissionParameter> getSubmissionParameters() {
        return createSubmissionParameters();
    }
    
    public void addSubmissionParameter(String name, boolean value) {
        createSubmissionParameters().add(new SubmissionParameter(name, value));
    }
    public void addSubmissionParameter(String name, String value) {
        createSubmissionParameters().add(new SubmissionParameter(name, value));
    }
    public void addSubmissionParameter(String name, Number value) {
        createSubmissionParameters().add(new SubmissionParameter(name, value));
    }
    
    /**
     * Create a {@code JobConfig} from a configuration map.
     * If {@code config} contains {@link JobProperties#CONFIG} and
     * it is an instance of {@code JobConfig} then it is returned.
     * <BR>
     * Otherwise a {@code JobConfig} object is created from other
     * {@code JobProperties} in {@code config}. If none exist then
     * an empty {@code JobConfig} is returned.
     * @param config Submission configuration.
     * @return JobConfig from {@code config}.
     * 
     * @see JobProperties
     */
    @SuppressWarnings("unchecked")
    public static JobConfig fromProperties(Map<String,? extends Object> config) {
        if (config.containsKey(CONFIG)) {
            Object cfg = config.get(CONFIG);
            if (JSON4JBridge.isJson4J(cfg)) {
                JsonObject json = JSON4JBridge.fromJSON4J(cfg);
                return fromJSON(json);
            }
        
            return getConfigEntry(config, CONFIG, JobConfig.class);
        }
        
        JobConfig jc = fromPrimitiveValues(config);

        if (config.containsKey(SUBMISSION_PARAMS)) {
            Map<String,Object> params = (Map<String,Object>) config.get(SUBMISSION_PARAMS);
            for (String name : params.keySet()) {
                jc.addSubmissionParameter(name, params.get(name).toString());
            }
        }
        
        if (config.containsKey(TRACING_LEVEL))
            jc.setTracing(Util.getConfigEntry(config, TRACING_LEVEL, Level.class));

        return jc;
    }
    
    /**
     * Works from a set of individual values in a config
     * and from a JSON config.
     */
    private static JobConfig fromPrimitiveValues(Map<String,? extends Object> config) {
        JobConfig jc = new JobConfig();
        if (config.containsKey(NAME))
            jc.setJobName(getConfigEntry(config, NAME, String.class));
        
        if (config.containsKey(GROUP))
            jc.setJobGroup(getConfigEntry(config, GROUP, String.class));
        
        if (config.containsKey(DATA_DIRECTORY))
            jc.setDataDirectory(getConfigEntry(config, DATA_DIRECTORY, String.class));
        
        if (config.containsKey(OVERRIDE_RESOURCE_LOAD_PROTECTION))
            jc.setOverrideResourceLoadProtection(Util.getConfigEntry(config, 
                    OVERRIDE_RESOURCE_LOAD_PROTECTION, Boolean.class));

        if (config.containsKey(PRELOAD_APPLICATION_BUNDLES))
            jc.setPreloadApplicationBundles(Util.getConfigEntry(config, 
                    PRELOAD_APPLICATION_BUNDLES, Boolean.class));
        
        return jc;
    }
    /**
     * Works from a set of individual values in a config
     * and from a JSON config.
     */
    private static JobConfig fromJSON(JsonObject config) {
        JobConfig jc = new JobConfig();
        if (config.has("jobName"))
            jc.setJobName(config.get("jobName").getAsString());
        
        if (config.has("jobGroup"))
            jc.setJobGroup(config.get("jobGroup").getAsString());
        
        if (config.has("dataDirectory"))
            jc.setDataDirectory(config.get("dataDirectory").getAsString());
        
        if (config.has("overrideResourceLoadProtection"))
            jc.setOverrideResourceLoadProtection(config.get("overrideResourceLoadProtection").getAsBoolean());

        if (config.has("preloadApplicationBundles"))
            jc.setPreloadApplicationBundles(config.get("preloadApplicationBundles").getAsBoolean());
        
        return jc;
    }
    
    private static String toTracingLevel(Level level) {
        int tli = level.intValue();
        String tls;
        if (tli == Level.OFF.intValue())
            tls = "off";
        else if (tli == Level.ALL.intValue())
            tls = "debug";
        else if (tli >= Level.SEVERE.intValue())
            tls = "error";
        else if (tli >= Level.WARNING.intValue())
            tls = "warn";
        else if (tli >= Level.INFO.intValue())
            tls = "info";
        else if (tli >= Level.FINE.intValue())
            tls = "debug";
        else if (tli >= Level.FINEST.intValue())
            tls = "trace";
        else
            tls = "trace";
        return tls;
    }
}
