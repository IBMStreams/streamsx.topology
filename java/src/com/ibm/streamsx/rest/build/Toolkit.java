package com.ibm.streamsx.rest.build;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

/**
 * 
 * An object describing an IBM Streams Toolkit
 * 
 */
public class Toolkit extends Element {
  @Expose 
  private String id;
  @Expose
  private String index;
  @Expose
  private String name;
  @Expose
  private String path;
  @Expose
  private String requiredProductVersion;
  @Expose
  private String resourceType;
  @Expose
  private String restid;
  @Expose
  private String version;

  final static List<Toolkit> createToolkitList(AbstractConnection sc, JsonObject gsonToolkitString) {
    if (gsonToolkitString.toString().isEmpty()) {
      return Collections.emptyList();
    }
    try {
      ToolkitsArray array = gson.fromJson(gsonToolkitString.toString(), ToolkitsArray.class);
      for (Element e: array.elements()){
        e.setConnection(sc);
      }

      return array.elements();
    }
    catch (JsonSyntaxException e) {
      return Collections.emptyList();
    }
  }

  final static List<Toolkit> createToolkitList(AbstractConnection sc, String uri) throws IOException {   
    return createList(sc, uri, ToolkitsArray.class);
  }

  final static Toolkit create(AbstractConnection sc, String uri) throws IOException {
    return create(sc, uri, Toolkit.class);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public String getRequiredProductVersion() {
    return requiredProductVersion;
  }

  public String getResourceType() {
    return resourceType;
  }

  public String getVersion() {
    return version;
  }

  public String getIndex() throws IOException {
    String index = connection().getResponseString(this.index);
    return index;
  }

  public boolean delete() throws IOException {
    return ((StreamsBuildService)connection()).deleteToolkit(this);
  }

  public static class Dependency {
    public Dependency(String name, String version) {
      this.name = name;
      this.version = version;
    }
    public String getName() {
      return name;
    }
    public String getVersion() {
      return version;
    }
    public String name;
    public String version;
  }

  public List<Dependency> getDependencies() throws Exception {
    List<Dependency> dependencies = new ArrayList();

    String index = getIndex();

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    InputStream is = new ByteArrayInputStream(index.getBytes("UTF-8"));
    Document doc = builder.parse(is);

    NodeList toolkitModelElementList = doc.getDocumentElement().getElementsByTagNameNS("http://www.ibm.com/xmlns/prod/streams/spl/toolkit", "toolkit");

    org.w3c.dom.Element toolkitModelElement = (org.w3c.dom.Element) toolkitModelElementList.item(0);

    NodeList dependenciesList = toolkitModelElement.getElementsByTagNameNS("http://www.ibm.com/xmlns/prod/streams/spl/toolkit", "dependency");
    int dependenciesCount = dependenciesList.getLength();
    for (int dependencyIndex = 0; dependencyIndex < dependenciesCount; ++dependencyIndex) {
      org.w3c.dom.Element dependencyElement = (org.w3c.dom.Element) dependenciesList.item(dependencyIndex);
      Node nameElement = dependencyElement.getElementsByTagNameNS("http://www.ibm.com/xmlns/prod/streams/spl/common", "name").item(0);
      Node versionElement = dependencyElement.getElementsByTagNameNS("http://www.ibm.com/xmlns/prod/streams/spl/common", "version").item(0);
      String name = nameElement.getTextContent();
      String version = versionElement.getTextContent();
      Dependency dependency = new Dependency(name, version);
      dependencies.add(dependency);
    }
    return dependencies;
  }

  /**
   * internal usage to get list of toolkits
   */
  private static class ToolkitsArray extends ElementArray<Toolkit> {
    @Expose
    private ArrayList<Toolkit> toolkits;

    @Override
    List<Toolkit> elements() { return toolkits; }
  }
}
