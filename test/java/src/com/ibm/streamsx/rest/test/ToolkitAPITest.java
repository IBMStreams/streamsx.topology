package com.ibm.streamsx.rest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.ibm.streamsx.rest.internal.ZipStream;

import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.rest.build.Toolkit;

import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import org.apache.http.client.fluent.Executor;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.internal.RestUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.client.fluent.Response;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import java.io.Reader;
import com.google.gson.Gson;
import java.io.InputStreamReader;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import java.net.URI;
import java.net.URL;

public class ToolkitAPITest {
  protected BuildService connection;
  String instanceName;
  String testType;

  @BeforeClass
  public static void maybeSkip() {
    assumeTrue(null == System.getenv("STREAMS_DOMAIN_ID"));
  }

  @Before
  public void setup() throws Exception {
    setupConnection();
    deleteToolkits();
  }

  @After
  public void deleteToolkits() throws Exception {
    Set<String> deleteNames = new HashSet();
    deleteNames.add(gamesToolkitName);
    deleteNames.add(cardsToolkitName);
    deleteNames.add(bingoToolkitName);

    List<Toolkit> deleteToolkits;
    do {

      List<Toolkit> toolkits = connection.getToolkits();
      deleteToolkits = toolkits.stream()
        .filter(tk -> deleteNames.contains(tk.getName()))
        .collect(Collectors.toList());
      
      for(Toolkit toolkit: deleteToolkits){
        toolkit.delete();
      }

      Thread.sleep(5000);

    } while (!deleteToolkits.isEmpty());
  }

  @Test
  public void testGetToolkits() throws Exception {    
    List<Toolkit> toolkits = connection.getToolkits();
    
    // We don't know what toolkits are present on the host, but
    // we can assume at very least the standard spl toolkit is present.
    assertNotNull(toolkits);
    assertTrue(toolkits.size() > 0);
    assertNotNull(toolkits.get(0).getName());
    assertNotNull(toolkits.get(0).getVersion());

    // Find the spl toolkit (I think there can only be one)
    assertEquals(1, toolkits.stream()
                            .filter(tk -> tk.getName().equals("spl"))
                            .count());
    
    //for (Toolkit tk: toolkits) {
    //  System.out.println(tk.getName() + " " + tk.getVersion());
    //}
  }

  @Test
  public void testPostToolkit() throws Exception {
    Toolkit bingo = connection.uploadToolkit(bingo0Path);
    assertNotNull(bingo);
    assertEquals(bingo.getName(), bingoToolkitName);
    assertEquals(bingo.getVersion(), bingo0Version);
    assertEquals(bingo.getRequiredProductVersion(), "4.2");
    assertEquals(bingo.getResourceType(), "toolkit");

    // We don't know what the values the following attributes will have,
    // but we verify that the expected attributes do at least have values
    assertNotNull(bingo.getPath());

    // Verify that the new toolkit is in the list of all toolkits
    List<Toolkit> toolkits = connection.getToolkits();
    waitForToolkit(bingoToolkitName);

    assertTrue(bingo.delete());
  }

  @Test
  public void testDeleteToolkit() throws Exception {
    Toolkit bingo = connection.uploadToolkit(bingo0Path);
    assertNotNull(bingo);
    assertEquals(bingo.getName(), bingoToolkitName);
    assertEquals(bingo.getVersion(), bingo0Version);

    waitForToolkit(bingo.getName(), Optional.of(bingo.getVersion()));

    // deleting once should succeed.
    assertTrue(bingo.delete());
    
    // Verify that it has been removed
    assertToolkitNotExists(bingoToolkitName);

    // deleting again should fail.
    assertFalse(bingo.delete());

    // RESTException if we try to get index on a deleted toolkit.
    try {
      bingo.getIndex();
      fail ("Expected RESTException");
    }
    catch (RESTException e) {
    }

    // Post it again, then find it in the list of toolkits and delete
    // it from the Toolkit object from the list.
    bingo = connection.uploadToolkit(bingo0Path);
    assertNotNull(bingo);
    assertEquals(bingo.getName(), bingoToolkitName);
    assertEquals(bingo.getVersion(), bingo0Version);

    List<Toolkit> foundToolkits = findMatchingToolkits(bingoToolkitName, Optional.of(bingo0Version));
    assertEquals(foundToolkits.size(), 1);
    assertTrue(foundToolkits.get(0).delete());    
  }

  @Test
  public void testGetIndex() throws Exception {
    Toolkit bingo = connection.uploadToolkit(bingo0Path);
    assertNotNull(bingo);
    assertEquals(bingoToolkitName, bingo.getName());
    assertEquals(bingo0Version, bingo.getVersion());

    String index = bingo.getIndex();
    assertNotNull(index);

    // try parsing the xml, and verify some of the content.
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    assertTrue(factory.isNamespaceAware());
    DocumentBuilder builder = factory.newDocumentBuilder();
    InputStream is = new ByteArrayInputStream(index.getBytes("UTF-8"));
    Document doc = builder.parse(is);

    NodeList toolkitModelElementList = doc.getDocumentElement().getElementsByTagNameNS("http://www.ibm.com/xmlns/prod/streams/spl/toolkit", "toolkit");
    assertNotNull(toolkitModelElementList);
    assertEquals(1, toolkitModelElementList.getLength());

    Node toolkitElementNode = toolkitModelElementList.item(0);
    assertTrue(toolkitElementNode.hasAttributes());
    NamedNodeMap attributes = toolkitElementNode.getAttributes();
    assertNotNull(attributes);
    Node nameNode = attributes.getNamedItem("name");
    assertNotNull(nameNode);
    assertEquals(bingoToolkitName, nameNode.getNodeValue());
    Node versionNode = attributes.getNamedItem("version");
    assertNotNull(versionNode);
    assertEquals(bingo0Version, versionNode.getNodeValue());    

    assertTrue(bingo.delete());
  }

  // Test posting different versions of a toolkit.  Posting a version
  // equal to one that is currently deployed should fail,
  // but posting a different version should succeed.
  @Test
  public void testPostMultipleVersions() throws Exception {
    List<Toolkit> toolkits = connection.getToolkits();
   
    // First post version 1.0.1
    Toolkit bingo1 = connection.uploadToolkit(bingo1Path);
    assertNotNull(bingo1);
    waitForToolkit(bingoToolkitName, Optional.of(bingo1Version));

    // Post version 1.0.1 again.  It should return Null
    assertNull(connection.uploadToolkit(bingo1Path));

    // Post verison 1.0.0.  It should succeed as it does not match any
    // existing version.
    Toolkit bingo0 = connection.uploadToolkit(bingo0Path);
    assertNotNull(bingo0);
    waitForToolkit(bingoToolkitName, Optional.of(bingo0Version));

    // Version 1.0.1 should still exist
    assertToolkitExists(bingoToolkitName, Optional.of(bingo1Version));
    assertToolkitNotExists(bingoToolkitName, Optional.of(bingo2Version));

    // Post version 1.0.2.  All three version continue to exist.
    Toolkit bingo2 = connection.uploadToolkit(bingo2Path);
    assertNotNull(bingo2);
    waitForToolkit(bingoToolkitName, Optional.of(bingo2Version));
    
    assertToolkitExists(bingoToolkitName, Optional.of(bingo0Version));
    assertToolkitExists(bingoToolkitName, Optional.of(bingo1Version));

    assertTrue(bingo0.delete());
    assertTrue(bingo1.delete());
    assertTrue(bingo2.delete());
  }

  // Test getting the dependencies of a toolkit.
  @Test
  public void testGetDependencies() throws Exception {
    // Games depends on both cards and bingo.

    Toolkit bingo = connection.uploadToolkit(bingo0Path);
    assertNotNull(bingo);

    Toolkit cards = connection.uploadToolkit(cardsPath);
    assertNotNull(cards);
    
    Toolkit games = connection.uploadToolkit(gamesPath);
    assertNotNull(games);

    // bingo and cards have no dependencies
    assertEquals(0, bingo.getDependencies().size());
    assertEquals(0, cards.getDependencies().size());

    List<Toolkit.Dependency> gamesDependencies = games.getDependencies();
    assertEquals(2, gamesDependencies.size());

    assertEquals("com.example.bingo", gamesDependencies.get(0).getName());
    assertEquals("[1.0.0,2.0.0)", gamesDependencies.get(0).getVersion());
    assertEquals("com.example.cards", gamesDependencies.get(1).getName());
    assertEquals("[1.0.0,1.1.0)", gamesDependencies.get(1).getVersion());

    assertTrue(games.delete());
    assertTrue(cards.delete());
    assertTrue(bingo.delete());
  }

  // Test posting from a bad path
  @Test
  public void testBadPath() throws IOException {
    // Path does not exist
    File notExists = new File(bingo0Path.getParent(), "fleegle_tk");
    try {
      connection.uploadToolkit(notExists);
      fail("IOException expected");
    }
    catch(IllegalArgumentException e) {
    }

    // Path is an individual file
    File toolkitXml = new File(bingo0Path, "toolkit.xml");
    try {
      connection.uploadToolkit(toolkitXml);
      fail("IOException expected");
    }
    catch(IllegalArgumentException e) {
    }

    // Path is malformed garbage
    File garbagePath = new File("./toolkits/bingo_tk0\000/snork");
    try {
      connection.uploadToolkit(garbagePath);
      fail("IllegalArgumentException expected");
    }
    catch(IllegalArgumentException e) {
    }

    // Not a toolkit directory.
    File notToolkit = bingo0Path.getParentFile();
    try {
      connection.uploadToolkit(notToolkit);
      fail("IOException expected");
    }
    catch(IllegalArgumentException e) {
    }
  }

  // Test getting a toolkit by id.
  @Test
  public void testGetTookit() throws Exception {
    Toolkit bingo = connection.uploadToolkit(bingo1Path);
    assertNotNull(bingo);
    waitForToolkit(bingoToolkitName, Optional.of(bingo1Version));

    Toolkit found = connection.getToolkit(bingo.getId());
    assertNotNull(found);
    assertEquals (bingoToolkitName, found.getName());
    assertEquals (bingo1Version, found.getVersion());
    assertEquals ("4.2", found.getRequiredProductVersion());
    assertEquals ("toolkit", found.getResourceType());

    // We don't know what value this attribute will have, but it should
    // have a value
    assertNotNull(found.getPath());

    // The ID is 'streams-toolkits'/name-version
    String toolkitId = "streams-toolkits/" + bingoToolkitName + "-" + bingo1Version;
    found = connection.getToolkit(toolkitId);
    assertNotNull(found);

    // Using just the name fails
    toolkitId = "streams-toolkits/" + bingoToolkitName;
    try {
      found = connection.getToolkit(toolkitId);
      fail ("Expected RESTException");
    }
    catch (RESTException e) {
    }
  }

  // Test the zip file creation class.  Zip a directory, then write it to a file,
  // unzip it, and compare it to the original directory.
  @Test
  public void testZip() throws Exception {
    Path tkpath = bingo0Path.toPath();
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    Path targetParent = Files.createTempDirectory(baseDir.toPath(), "tz");
    try {
      Path target = new File(targetParent.toFile(),"tk.zip").toPath();
      try (InputStream is = ZipStream.fromPath(tkpath)) {
        Files.copy(is, target);
      }

      // Unzip the zip file with system tools and compare with the source
      // directory.  
      StringBuilder command = new StringBuilder();
      command.append("cd '").append(targetParent.toFile().getAbsolutePath()).append("' && ");
      command.append("unzip -qq tk.zip && ");
      command.append("diff -r bingo_tk0 '").append(bingo0Path.getAbsolutePath()).append("'");
      System.out.println(command);

      ProcessBuilder builder = new ProcessBuilder("bash","-c",command.toString());
      Process process = builder.start();
      int exitCode = process.waitFor();
    
      copy (process.getInputStream(), System.out);
      copy (process.getErrorStream(), System.err);

      assertEquals(0, exitCode);

    }
    finally {
      // Delete the temp files and contents this when test is done.  
      Files.walkFileTree(targetParent, new FileDeleter());
    }
  }

  static private void copy(InputStream is, OutputStream os) throws IOException {
    byte[] block = new byte[8192];
    int count = 0;
    while (count != -1) {
      os.write(block, 0, count);
      count = is.read(block);
    }
  }

  protected void setupConnection() throws Exception {
    if (connection == null) {
      testType = "DISTRIBUTED";
            
      String icpdUrl = System.getenv("ICPD_URL");
      
      String authUrl = icpdUrl + "/icp4d-api/v1/authorize";
      boolean allowInsecure = true;
      Executor executor = RestUtils.createExecutor(allowInsecure);

      JsonObject authParams = new JsonObject();
      authParams.addProperty("username","admin");
      authParams.addProperty("password","password");

      Request request = Request.Post(authUrl)
        .addHeader("accept", ContentType.APPLICATION_JSON.getMimeType())
        .bodyString(authParams.toString(), ContentType.APPLICATION_JSON);
      
      Response response = executor.execute(request);
      JsonObject jsonResponse = gsonFromResponse(response.returnResponse());
     

      //String bearerToken = jsonResponse.get("token").getAsString();
      String bearerToken = jstring(jsonResponse, "token");

      // This is only the token to get the token.
      String instanceName = System.getenv("STREAMS_INSTANCE_ID");
      String detailsUrl = icpdUrl + "/zen-data/v2/serviceInstance/details?displayName=" + instanceName;


      request = Request.Get(detailsUrl)
        .addHeader("accept", ContentType.APPLICATION_JSON.getMimeType())
        .addHeader("Authorization","Bearer " + bearerToken);

      response = executor.execute(request);
      JsonObject sr = gsonFromResponse(response.returnResponse());
      JsonObject sro = jobject(sr, "requestObj");
      String serviceId = jstring(sro, "ID");

      String serviceTokenUrl = icpdUrl + "/zen-data/v2/serviceInstance/token";
      
      JsonObject pd = new JsonObject();
      pd.addProperty("serviceInstanceId", serviceId);
      request = Request.Post(serviceTokenUrl)
        .addHeader("accept", ContentType.APPLICATION_JSON.getMimeType())
        .addHeader("Authorization","Bearer " + bearerToken)
        .bodyString(pd.toString(), ContentType.APPLICATION_JSON);

      response = executor.execute(request);
      jsonResponse = gsonFromResponse(response.returnResponse());
      String serviceToken = jstring(jsonResponse, "AccessToken");
      
      JsonObject sca = jobject(sro, "CreateArguments");
      JsonObject connectionInfo = jobject(sca, "connection-info");
      String buildEndpoint = jstring(connectionInfo, "externalBuildEndpoint");

        
      URI icpdURI = new URI(icpdUrl);

      URI internalURI = new URI(buildEndpoint);

      String externalHost = icpdURI.getHost();

      URI externalURI = new URI(internalURI.getScheme(), 
                                internalURI.getUserInfo(), 
                                externalHost, 
                                internalURI.getPort(), 
                                internalURI.getPath(),
                                internalURI.getQuery(), 
                                internalURI.getFragment());

      String buildUrl = externalURI.toURL().toExternalForm();

      connection = BuildService.of(buildUrl, serviceToken);
      if (!sslVerify())
	connection.allowInsecureHosts();      
    }
  }

  // TODO remove this
    private static JsonObject gsonFromResponse(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
            final String errorInfo;
            if (entity != null)
                errorInfo = " -- " + EntityUtils.toString(entity);
            else
                errorInfo = "";
            throw new IllegalStateException(
                    "Unexpected HTTP resource from service:"
                            + response.getStatusLine().getStatusCode() + ":" +
                            response.getStatusLine().getReasonPhrase() + errorInfo);
        }

        if (entity == null)
            throw new IllegalStateException("No HTTP resource from service");
        Reader r = new InputStreamReader(entity.getContent());
        JsonObject jsonResponse = new Gson().fromJson(r, JsonObject.class);
        EntityUtils.consume(entity);
        return jsonResponse;
    }

  public static boolean sslVerify() {
    String v = System.getProperty("topology.test.SSLVerify");
    if (v == null)
      return true;
    
    return Boolean.valueOf(v);
  }
  
  private List<Toolkit> findMatchingToolkits(String name, Optional<String> version) throws IOException {
    List<Toolkit> matches = connection.getToolkits().stream()
      .filter(tk -> (tk.getName().equals(name) && 
                     (!version.isPresent() || tk.getVersion().equals(version.get()))))
      .collect(Collectors.toList());
    return matches;
  }

  private void waitForToolkit(String name) throws Exception {
    waitForToolkit(name, Optional.empty());
  }

  private void waitForToolkit(String name, Optional<String> version) throws Exception {
    waitForToolkit(name, version, 60);
  }

  private void waitForToolkit(String name, Optional<String> version, int retries) throws Exception {
    int retry = 0;
    boolean found = false;
    while (!found && retry < retries) {
      List<Toolkit> matches = findMatchingToolkits(name, version);
      if (matches.isEmpty()) {
        Thread.sleep(1000);
        ++retry;
      }
      else {
        found = true;
      }
    }

    if (!found) {
      fail("Toolkit " + name + " not found");
    }
  }

  private void assertToolkitExists(String name) throws IOException {
    assertToolkitExists(name, Optional.empty());
  }

  private void assertToolkitExists(String name, Optional<String> version) throws IOException {
    List<Toolkit> matches = findMatchingToolkits(name, version);
    assertTrue (matches.size() >= 1);
  }

  private void assertToolkitNotExists(String name) throws IOException {
    assertToolkitNotExists(name, Optional.empty());
  }

  private void assertToolkitNotExists(String name, Optional<String> version) throws IOException {
    List<Toolkit> matches = findMatchingToolkits(name, version);
    assertEquals(0, matches.size());
  }

  private static class FileDeleter extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
      if (e == null) {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
      else {
        throw e;
      }
    }
  }

  static {
    // We can't really use a relative path here, because it is relative
    // to the location where the test is run.
    String toolkitDir = System.getProperty("topology.test.toolkit.dir", "../python/rest/toolkits/");
    bingo0Path = new File(toolkitDir, "bingo_tk0");
    bingo1Path = new File(toolkitDir, "bingo_tk1");
    bingo2Path = new File(toolkitDir, "bingo_tk2");
    cardsPath = new File(toolkitDir, "cards_tk");
    gamesPath = new File(toolkitDir, "games_tk");
  }

  private static final File bingo0Path;
  private static final File bingo1Path;
  private static final File bingo2Path;
  private static final File cardsPath;
  private static final File gamesPath;
  private static final String bingoToolkitName = "com.example.bingo";
  private static final String cardsToolkitName = "com.example.cards";
  private static final String gamesToolkitName = "com.example.games";
  private static final String bingo0Version = "1.0.0";
  private static final String bingo1Version = "1.0.1";
  private static final String bingo2Version = "1.0.2";
}
