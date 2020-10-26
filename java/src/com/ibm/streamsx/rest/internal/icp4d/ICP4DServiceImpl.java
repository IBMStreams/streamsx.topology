package com.ibm.streamsx.rest.internal.icp4d;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.internal.AbstractConnection;
import com.ibm.streamsx.rest.internal.RestUtils;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * Abstracts the REST API of the CP4D.
 * Use this class only when CP4D >= 3.5 and Streams >= 5.5.
 * 
 * @since 1.17
 */
public class ICP4DServiceImpl extends AbstractConnection implements ICP4DService {

    private final String jobsRestUrl;
    private final String spacesRestUrl;
    private final String apiBaseUrl;
    private final String streamsServiceName;
    private final boolean externalClient;
    private final Function<Executor,String> authenticator;


    public ICP4DServiceImpl (JsonObject service, final boolean verify, Function<Executor,String> authenticator) {
        super(!verify);
        this.authenticator = authenticator;
        this.externalClient = GsonUtilities.jboolean (service, StreamsKeys.EXTERNAL_CLIENT);
        String clusterIp = removeTrailing (GsonUtilities.jstring (service, StreamsKeys.CLUSTER_IP), "/");
        int clusterPort = GsonUtilities.jint (service, StreamsKeys.CLUSTER_PORT);
        this.streamsServiceName = GsonUtilities.jstring (service, StreamsKeys.SERVICE_NAME);
        // use URL class to ensure we have a valid URL
        try {
            this.apiBaseUrl = (new URL("https", clusterIp, clusterPort, "")).toExternalForm();
            this.jobsRestUrl = (new URL("https", clusterIp, clusterPort, "/v2/jobs")).toExternalForm();
            this.spacesRestUrl = (new URL("https", clusterIp, clusterPort, "/v2/spaces")).toExternalForm();
//            System.out.println("this.apiBaseUrl = " + this.apiBaseUrl);
//            System.out.println("this.jobsRestUrl = " + this.jobsRestUrl);
//            System.out.println("this.spacesRestUrl = " + this.spacesRestUrl);
//            System.out.println("Auth = " + this.authenticator.apply(executor));
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Removes a suffix from a String s
     * @param s the string
     * @param suffix the suffix to be removed if present
     * @return 
     */
    private static String removeTrailing (String s, String suffix) {
        if (s == null || suffix == null || !s.endsWith(suffix)) {
            return s;
        }
        return s.substring(0, s.length() - suffix.length());
    }

    /**
     * @see com.ibm.streamsx.rest.internal.icp4d.ICP4DService#getJobsRestUrl()
     */
    @Override
    public String getJobsRestUrl() {
        return this.jobsRestUrl;
    }

    /**
     * @see com.ibm.streamsx.rest.internal.icp4d.ICP4DService#getSpacesRestUrl()
     */
    @Override
    public String getSpacesRestUrl() {
        return this.spacesRestUrl;
    }

    /**
     * true if the client is external to the CP4D. 
     * @return
     */
    @Override
    public boolean isExternalClient() {
        return externalClient;
    }

    /**
     * @throws IOException
     */
    @Override
    public void test() throws IOException {
        final String randomSpaceName = RestUtils.randomHex(20);
        getSpaceForName (randomSpaceName);
    }

    /**
     * Gets the deployment space given by its name.
     * @param spaceName the space name
     * @return the first space instance with matching name or null if the space name doesn't exist.
     * @throws IOException the REST request failed
     */
    private DeploymentSpace getSpaceForName (String spaceName) throws IOException {
        final String queryUrl = this.spacesRestUrl + "?name=" + URLEncoder.encode (spaceName, StandardCharsets.UTF_8.name());
        List<DeploymentSpace> spaces = DeploymentSpace.createSpaceList (this, queryUrl);
        //        System.out.println("--> space list size = " + spaces.size());
        for (DeploymentSpace space: spaces) {
            // return the first match
            if (spaceName.equals(space.getName())) {
                //                System.out.println(space);
                return space;
            }
        }
        // not found
        return null;
    }

    /**
     * Creates a new deployment space with the given name.
     * @param spaceName the name for the space
     * @return The created space REST object
     * @throws IOException 
     */
    private DeploymentSpace createSpace (String spaceName) throws IOException {
        
        /* POST /v2/spaces
         {
           "name": "string",
           "description": "string",
           "storage": {
             "resource_crn": "string",
             "delegated": false
           },
           "compute": [
             {
               "name": "string",
               "crn": "string"
             }
           ],
           "tags": [
             "string"
           ]
         }
         */
        final String restUrl = this.getSpacesRestUrl();
        JsonObject body = new JsonObject();
        body.addProperty("name", spaceName);
        body.addProperty("description",  "Created by streamsx.topology");
        String bodyStr = body.toString();
        Request post = Request.Post (restUrl)
                .addHeader ("Authorization", getAuthorization())
                .bodyString (bodyStr, ContentType.APPLICATION_JSON);

//        System.out.println("... createSpace body = " + body);
        JsonObject response = RestUtils.requestGsonResponse (getExecutor(), post);
//        System.out.println("... createSpace POST response = " + response);
        DeploymentSpace space = DeploymentSpace.create (this, response);
//        System.out.println("... createSpace returned space = " + space);
        return space;
    }

    /**
     * Gets the spaceId for a deployment space given by its name.
     * @param spaceName the space name
     * @return the space ID of the first space instance with matching name or null if the space name doesn't exist.
     * @throws IOException 
     */
    @Override
    public String getSpaceIdForName (String spaceName) throws IOException {
        DeploymentSpace space = getSpaceForName (spaceName);
        return space != null? space.getId(): null;
    }

    /**
     * Gets or creates a CP4D deployment space.
     * @param spaceName the name of the deployment space
     * @return the Space object.
     * @see com.ibm.streamsx.rest.internal.icp4d.ICP4DService#getOrCreateSpace(java.lang.String)
     */
    @Override
    public DeploymentSpace getOrCreateSpace (String spaceName) throws IOException {
        DeploymentSpace space = getSpaceForName (spaceName);
        if (space != null) {
            return space;
        }
        space = createSpace (spaceName); 
        return space;
    }

    /**
     * Gets or creates a CP4D job description associated with either a project or a deployment space.
     * @param jobName the name of the job
     * @param spaceId the space_id to associate the job with a deployment space. <tt>projectId</tt> must be null.
     * @param projectId the project_id to associate the job with a project. <tt>space_id</tt> must be null.
     * @return the Job object.
     */
    @Override
    public JobDescription getOrCreateJobDescription (String jobName, String spaceId, String projectId) throws IOException {
        if (spaceId == null && projectId == null) throw new IllegalArgumentException ("at least one of spaceId or projectId must be specified");
        if (spaceId != null && projectId != null) throw new IllegalArgumentException ("only one of spaceId or projectId can be specified, not both");
        final String queryUrl;
        if (spaceId != null) {
            queryUrl = getJobsRestUrl() + "?space_id=" +  URLEncoder.encode (spaceId, StandardCharsets.UTF_8.name());
        }
        else {
            queryUrl = getJobsRestUrl() + "?project_id=" +  URLEncoder.encode (projectId, StandardCharsets.UTF_8.name());
        }
        List<JobDescription> jobs = JobDescription.createJobList (this, queryUrl, jobName);
        if (jobs.size() > 0) {
            // return first match
            JobDescription job0 = jobs.get(0);
            job0.getMetaData().setProject_id (projectId);
            job0.getMetaData().setSpace_id (spaceId);
            return job0;
        }
        // create Job
        return createJobDescription (jobName, spaceId, projectId);
    }


    /**
     * Creates a Job run for a job description via REST.
     * 
     * @param jobDescrition the job description
     * @param sabUrl the URL of the Streams application bundle
     * @param jobConfigOverlaysArray the job configurations
     * 
     * @return a JobRunConfiguration instance
     * 
     * @see com.ibm.streamsx.rest.internal.icp4d.ICP4DService#createJobRun(com.ibm.streamsx.rest.internal.icp4d.JobDescription, java.lang.String)
     * @throws IOException 
     */
    @Override
    public JobRunConfiguration createJobRun (JobDescription jobDescrition, String sabUrl, JsonArray jobConfigOverlaysArray) throws IOException {
        // POST /v2/jobs/{job_id}/runs
        final String restUrl = getJobsRestUrl() + "/" + jobDescrition.getAssetId() + "/runs?" + jobDescrition.createAssociatedWithQueryParam();
//        System.out.println ("createJobRun: restUrl = " + restUrl);
        /*
         {
           "job_run": {
             "configuration": {
               "streamsInstance": "sample-streams",
               "application": "https://cluster.ip/sab_url",
               "jobConfigurationOverlay": {
                 "jobConfigOverlays": [
                   {
                     "jobConfig": {
                       "submissionParameters": [
                         {
                           "name": "somenamespace::SomeMainComposite",
                           "value": "1"
                         }
                       ]
                     }
                   }
                 ]
               }
             }
           }
         }
         */
        JsonObject configuration = new JsonObject();
        configuration.addProperty ("streamsInstance", this.streamsServiceName);
        configuration.addProperty ("application", sabUrl);
        JsonObject jobConfigurationOverlay = new JsonObject();
        jobConfigurationOverlay.add ("jobConfigOverlays", jobConfigOverlaysArray);
        configuration.add ("jobConfigurationOverlay", jobConfigurationOverlay);
        
        JsonObject job_run = new JsonObject();
        job_run.add ("configuration", configuration);
        JsonObject body = new JsonObject();
        body.add("job_run", job_run);
        String bodyStr = body.toString();
        Request post = Request.Post (restUrl)
                .addHeader ("Authorization", getAuthorization())
                .bodyString (bodyStr, ContentType.APPLICATION_JSON);

//        System.out.println("... createJobRun body = " + body);
        JsonObject response = RestUtils.requestGsonResponse (getExecutor(), post);
//        System.out.println("... createJobRun POST response = " + response);
        if (!response.has("entity")) {
            throw new IllegalStateException ("no \"entity\" member in response of create JobRun");
        }
        JsonObject entity = response.get("entity").getAsJsonObject();
        if (!entity.has("job_run")) {
            throw new IllegalStateException ("no \"entity\"->\"job_run\" member in response of create JobRun");
        }
        JsonObject jobRun = entity.get("job_run").getAsJsonObject();
        if (!jobRun.has("configuration")) {
            throw new IllegalStateException ("no \"entity\"->\"job_run\"->\"configuration\" member in response of create JobRun");
        }
        JsonObject jobConfiguration = jobRun.get("configuration").getAsJsonObject();
//        System.out.println("... createJobRun jobConfiguration = " + jobConfiguration);
        return JobRunConfiguration.create (this, jobConfiguration);
        
//      response body
//        {
//            "entity": {
//              "job_run": {
//                "configuration": {
//                  "application": "https://nbgr55-cpd-nbgr55.apps.cpstreamsx2.cp.fyre.ibm.com:443/streams_build_service/v1/namespaces/nbgr55/instances/sample-streams/builds/122/artifacts/0/applicationbundle",
//                  "env_type": "streams",
//                  "env_variables": [],
//                  "jobConfigurationOverlay": {
//                    "jobConfigOverlays": [
//                      {
//                        "deploymentConfig": {
//                          "parallelRegionConfig": {
//                            "fusionType": "channelIsolation"
//                          }
//                        },
//                        "jobConfig": {
//                          "jobName": "MyTestJob"
//                        }
//                      }
//                    ]
//                  },
//   ==>               "jobId": "102",
//                  "namespace": "nbgr55",
//                  "serviceInstance": "1602644000868858",
//                  "streamsInstance": "sample-streams"
//                },
//                "isScheduledRun": false,
//                "job_name": "namespace1__TestApplication1",
//                "job_ref": "6fb3a4ec-fd1e-462d-bb72-32e82aeee713",
//                "job_type": "streams",
//                "space_name": "rolefs space",
//                "state": "Running"
//              }
//            },
//            "href": "/v2/assets/31c35366-3d79-4c96-ae22-bfa44ce6d711?space_id=54b262a0-cf67-4ac9-82bb-69caa8cabcae",
//            "metadata": {
//              "asset_attributes": [
//                "job_run"
//              ],
//              "asset_category": "USER",
//              "asset_id": "31c35366-3d79-4c96-ae22-bfa44ce6d711",
//              "asset_state": "available",
//              "asset_type": "job_run",
//              "catalog_id": "880ab3eb-a7c6-4155-8341-5f9513fb6838",
//              "created": 1602768721478,
//              "created_at": "2020-10-15T13:32:01Z",
//              "description": "created by streamsx.topology for namespace1__TestApplication1 application submissions",
//              "name": "MyTestJob",
//              "origin_country": "us",
//              "owner_id": "1000330999",
//              "rating": 0,
//              "rov": {
//                "collaborator_ids": {},
//                "mode": 0
//              },
//              "size": 0,
//              "space_id": "54b262a0-cf67-4ac9-82bb-69caa8cabcae",
//              "tags": [],
//              "total_ratings": 0,
//              "usage": {
//                "access_count": 0,
//                "last_access_time": 1602768734098,
//                "last_accessed_at": "2020-10-15T13:32:14Z",
//                "last_accessor_id": "1000330999",
//                "last_update_time": 1602768734098,
//                "last_updated_at": "2020-10-15T13:32:14Z",
//                "last_updater_id": "1000330999"
//              },
//              "version": 2
//            }
//          }
    }

    /**
     * creates a job description associated either with a project or a space
     * @param jobName The name of the job - alphanumeric characters only
     * @param spaceId the space ID of the space when associated with a space
     * @param projectId the project ID of the project when associated with a project
     * @return the creates JobDescription REST primitive
     * @throws IOException
     */
    private JobDescription createJobDescription (String jobName, String spaceId, String projectId) throws IOException {
        if (spaceId == null && projectId == null) throw new IllegalArgumentException ("at least one of spaceId or projectId must be specified");
        if (spaceId != null && projectId != null) throw new IllegalArgumentException ("only one of spaceId or projectId can be specified, not both");
        // POST /v2/jobs?project_id={project_id}
        // POST /v2/jobs?space_id={space_id}
        /*
        {
          "job": {
            "asset_ref_type": "streams",
            "name": "test job",
            "configuration": {
              "streamsInstance": "sample-streams",
              "application": "https://cluster.ip/sab_url",
              "jobConfigurationOverlay": {
                "jobConfigOverlays": [
                  {
                    "jobConfig": {
                      "submissionParameters": [
                        {
                          "name": "somenamespace::SomeMainComposite",
                          "value": "1"
                        }
                      ]
                    }
                  }
                ]
              }
            }
          }
        }
         */
        final String restUrl;
        if (spaceId != null) {
            restUrl = getJobsRestUrl() + "?space_id=" +  URLEncoder.encode (spaceId, StandardCharsets.UTF_8.name());
        }
        else {
            restUrl = getJobsRestUrl() + "?project_id=" +  URLEncoder.encode (projectId, StandardCharsets.UTF_8.name());
        }

        JsonObject jobParam = new JsonObject();
        jobParam.addProperty("asset_ref_type", JobDescription.STREAMS_ASSET_REF_TYPE);
        jobParam.addProperty("name", jobName);
        jobParam.addProperty("description", "created by streamsx.topology for application submissions");
        JsonObject configuration = new JsonObject();
        configuration.addProperty("streamsInstance", this.streamsServiceName);
        jobParam.add ("configuration", configuration);
        JsonObject body = new JsonObject();
        body.add("job", jobParam);
        String bodyStr = body.toString();
        Request post = Request.Post (restUrl)
                .addHeader ("Authorization", getAuthorization())
                .bodyString (bodyStr, ContentType.APPLICATION_JSON);

//        System.out.println("... createJob body = " + body);
        JsonObject response = RestUtils.requestGsonResponse (getExecutor(), post);
        /*
            Response
            
            {
              "metadata": {
                "rov": {
                  "mode": 0,
                  "collaborator_ids": {}
                },
                "space_id": "815e30bb-e050-4e6c-957d-f9559be364d0",
                "usage": {
                  "last_updated_at": "2020-10-09T17:36:43Z",
                  "last_updater_id": "1000330999",
                  "last_update_time": 1602265003135,
                  "last_accessed_at": "2020-10-09T17:36:43Z",
                  "last_access_time": 1602265003135,
                  "last_accessor_id": "1000330999",
                  "access_count": 0
                },
                "name": "test job",
                "description": "",
                "tags": [],
                "asset_type": "job",
                "origin_country": "us",
                "rating": 0,
                "total_ratings": 0,
                "catalog_id": "319ef586-079b-4a89-855e-6d7d2c76f91a",
                "created": 1602265003135,
                "created_at": "2020-10-09T17:36:43Z",
                "owner_id": "1000330999",
                "size": 0,
                "version": 2,
                "asset_state": "available",
                "asset_attributes": ["job"],
                "asset_id": "bc2cd8fa-2b3a-4cfe-b30a-f4eb0f34b3ea",
                "asset_category": "USER"
              },
              "entity": {
                "job": {
                  "asset_ref_type": "streams",
                  "configuration": {
                    "streamsInstance": "sample-streams",
                    "application": "https://cluster-ip/sab_url",
                    "jobConfigurationOverlay": {
                      "jobConfigOverlays": [
                        {
                          "jobConfig": {
                            "submissionParameters": [
                              {
                                "name": "somenamespace::SomeMainComposite",
                                "value": "1"
                              }
                            ]
                          }
                        }
                      ]
                    },
                    "env_type": "streams",
                    "env_variables": []
                  },
                  "last_run_initiator": "None",
                  "last_run_time": "",
                  "last_run_status": "None",
                  "last_run_status_timestamp": 0,
                  "space_name": "my space"
                }
              },
              "href": "/v2/assets/bc2cd8fa-2b3a-4cfe-b30a-f4eb0f34b3ea?space_id=815e30bb-e050-4e6c-957d-f9559be364d0",
              "asset_id": "bc2cd8fa-2b3a-4cfe-b30a-f4eb0f34b3ea"
            }
         */
//        System.out.println("... createJob POST response = " + response);
        JobDescription createdJob = JobDescription.create (this, response);
        // these fields are not included in the response; set them manually:
        createdJob.getMetaData().setProject_id (projectId);
        createdJob.getMetaData().setSpace_id (spaceId);
//        System.out.println("... createJob returned job = " + createdJob);
        return createdJob;
    }


    /**
     * Returns the Authorization header without the <tt>Authorization:</tt> keyword.
     */
    @Override
    public String getAuthorization() {
        return authenticator.apply(getExecutor());
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", ");
        sb.append((externalClient? "ext: ": "int: "));
        sb.append(apiBaseUrl);
        return sb.toString();
    }
}
