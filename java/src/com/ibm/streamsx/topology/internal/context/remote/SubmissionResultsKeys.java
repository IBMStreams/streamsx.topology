package com.ibm.streamsx.topology.internal.context.remote;

/**
 * Keys used in the JSON results file to keep track of artifacts created during submission.
 * The results file is read by Python, and any other sources who need information returned 
 * from a Java submit.
 */
public interface SubmissionResultsKeys {
    /**
     * The root of the created toolkit.
     */
    String TOOLKIT_ROOT = "toolkitRoot";
    
    /**
     * The file path of the created toolkit archive.
     */
    String ARCHIVE_PATH = "archivePath";
    
    /**
     * The file path of the compiled application bundle.
     */
    String BUNDLE_PATH = "bundlePath";
    
    /**
     * The job id of the submitted job.
     */
    String JOB_ID = "jobId";
    
    /**
     * The file path of the Job config file.
     */
    String JOB_CONFIG_PATH = "jobConfigPath";
    
    /**
     * JSON object holding metrics related to build/job submission.
     */
    String SUBMIT_METRICS = "submitMetrics";
    String TIME_MS_SUFFIX = "Time_ms";
            
    /**
     * Fields in in SUBMIT_METRICS.
     * All long values.
     * All times in milliseconds.
     */
       
    String SUBMIT_ARCHIVE_SIZE = "buildArchiveSize";
    String SUBMIT_UPLOAD_TIME = "buildArchiveUpload" + TIME_MS_SUFFIX;
    String SUBMIT_TOTAL_BUILD_TIME = "totalBuild" + TIME_MS_SUFFIX;
    String SUBMIT_JOB_TIME = "jobSubmission" + TIME_MS_SUFFIX;
    
    /**
     * Separate build state times in SUBMIT_METRICS
     */
    String SUBMIT_BUILD_STATE_PREFIX = "buildState_";
    static String buildStateMetricKey(String state) {
        return  SUBMIT_BUILD_STATE_PREFIX + state + TIME_MS_SUFFIX;
    }
    
    /**
     * URLs for Streams console application dashboard.
     */
    String CONSOLE_APPLICATION_URL = "console.application.url";
    String CONSOLE_APPLICATION_JOB_URL = "console.application.job.url";
}
