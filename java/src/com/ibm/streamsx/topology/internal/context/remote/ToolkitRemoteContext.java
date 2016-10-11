package com.ibm.streamsx.topology.internal.context.remote;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

public class ToolkitRemoteContext implements RemoteContext<File> {

    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    public Future<File> submit(JsonObject submission) throws Exception {
        // TODO Auto-generated method stub
        return null;
    } 
    
    private void createNamespaceFile(File toolkitRoot, JsonObject json, String suffix, String content)
            throws IOException {

        String namespace = GsonUtilities.jstring(json, "namespace");
        String name = (String) json.get("name");

        File f = new File(toolkitRoot,
                namespace + "/" + name + "." + suffix);
        PrintWriter splFile = new PrintWriter(f, "UTF-8");
        splFile.print(content);
        splFile.flush();
        splFile.close();
    }

    private void makeDirectoryStructure(File toolkitRoot, String namespace)
            throws Exception {

        File tkNamespace = new File(toolkitRoot, namespace);
        File tkImplLib = new File(toolkitRoot, "impl/lib");
        File tkEtc = new File(toolkitRoot, "etc");
        File tkOpt = new File(toolkitRoot, "opt");

        tkImplLib.mkdirs();
        tkNamespace.mkdirs();
        tkEtc.mkdir();
        tkOpt.mkdir();
    }
}
