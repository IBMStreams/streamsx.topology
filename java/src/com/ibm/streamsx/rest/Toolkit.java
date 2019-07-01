package com.ibm.streamsx.rest;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
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

  static Toolkit from_path(AbstractStreamsConnection sc, String path) {
    // TODO verify path is readable, is directory
    // possibly verify that the directory looks like a toolkit


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
