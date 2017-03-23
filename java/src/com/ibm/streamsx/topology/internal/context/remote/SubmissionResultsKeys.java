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
}
