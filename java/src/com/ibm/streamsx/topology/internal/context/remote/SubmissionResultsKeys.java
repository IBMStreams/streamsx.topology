package com.ibm.streamsx.topology.internal.context.remote;

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
    String BUNDLE_PATH = "bundleRoot";
    
    /**
     * The job id of the submitted job.
     */
    String JOB_ID = "jobId";
}
