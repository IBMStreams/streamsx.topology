package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Toolkit;

public class ToolkitAPITest {
  protected StreamsConnection connection;
    String instanceName;
    Instance instance;
    String testType;

  @Before 
  public void setup() throws Exception {
    setupConnection();
  }

  @After
  public void deleteToolkits() throws Exception {
    // TODO delete possibly leftover toolkits.
    // also maybe @Before?
  }

  @Test
  public void testSetupIntance() throws Exception {
    // TODO remove this test, it is useless
    setupInstance();
  }

  @Test
  public void testGetToolkits() throws Exception {
    List<Toolkit> toolkits = connection.getToolkits();
    
    // We don't know what toolkits are present on the host, but
    // we can assume at very list the standard spl toolkit is present.
    assertNotNull(toolkits);
    assertTrue(toolkits.size() > 0);
    assertNotNull(toolkits.get(0).getName());
    assertNotNull(toolkits.get(0).getVersion());

    // Find the spl toolkit (I think there can only be one)
    assertEquals(1, toolkits.stream()
                            .filter(tk->tk.getName().equals("spl"))
                            .count());
    
    for (Toolkit tk: toolkits) {
      System.out.println(tk.getName() + " " + tk.getVersion());
    }
  }

  @Test
  public void testPostToolkit() throws Exception {
    Toolkit bingo = Toolkit.from_path(connection, bingo_0_path);
    assertNotNull(bingo);
    assertEquals(bingo.getName(), bingo_tookit_name);
    assertEquals(bingo.getVersion(), bingo_0_version);
  }

  protected void setupConnection() throws Exception {
    if (connection == null) {
      testType = "DISTRIBUTED";
      
      instanceName = System.getenv("STREAMS_INSTANCE_ID");
      if (instanceName != null)
	System.out.println("InstanceName: " + instanceName);
      else
	System.out.println("InstanceName: assumng single instance");
      
      
      // TODO we need to change the way we get the URL for Cloud
      // pak for data.
      String buildUrl = System.getenv("STREAMS_BUILD_URL");
      assertNotNull("set STREAMS_BUILD_URL to run this test", buildUrl);
      System.out.println("build URL: " + buildUrl);
      connection = StreamsConnection.createInstance(null, null, null, buildUrl);
      
      if (!sslVerify())
	connection.allowInsecureHosts(true);
    }
  }

  protected void setupInstance() throws Exception {
    setupConnection();
    
    if (instance == null) {
      if (instanceName != null) {
	instance = connection.getInstance(instanceName);
      } else {
	List<Instance> instances = connection.getInstances();
	assertEquals(1, instances.size());
	instance = instances.get(0);
      }
      // don't continue if the instance isn't started
      assumeTrue(instance.getStatus().equals("running"));
    }
  }

  public static boolean sslVerify() {
    String v = System.getProperty("topology.test.SSLVerify");
    if (v == null)
      return true;
    
    return Boolean.valueOf(v);
  }

  private static final String bingo_0_path = "../python/rest/toolkits/bingo_tk0";
  private static final String bingo_toolkit_name = "com.example.bingo";
  private static final String bingo_0_version = "1.0.0";
}
