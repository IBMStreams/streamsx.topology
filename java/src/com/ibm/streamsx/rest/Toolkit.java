package com.ibm.streamsx.rest;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  private String name;
  @Expose
  private String version;
  // TODO the rest of the attributes

  final static List<Toolkit> createToolkitList(AbstractStreamsConnection sc, JsonObject gsonToolkitString) {
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

  final static List<Toolkit> createToolkitList(AbstractStreamsConnection sc, String uri)
       throws IOException {        
        return createList(sc, uri, ToolkitsArray.class);
  }  

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public boolean delete() {
    // TODO
    return false;
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
