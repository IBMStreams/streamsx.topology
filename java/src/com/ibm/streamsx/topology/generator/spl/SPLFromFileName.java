package com.ibm.streamsx.topology.generator.spl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import com.ibm.json.java.JSONObject;

/**
 * Given the path of a file containing the JSON representation of a graph,
 * read the file, generate the SPL, and write it to the specified file. All
 * paths should be absolute.
 * @author wcmarsha
 *
 */
public class SPLFromFileName {

    public static void main(String[] args) throws IOException {
        String JSONPath = args[0];
        String SPLPath = args[1];
        
        File JSONFile = new File(JSONPath);
        if(!JSONFile.exists()){
            throw new FileNotFoundException("File " + JSONPath + " does not exist");
        }
        FileInputStream fis= new FileInputStream(JSONFile);
        /*byte[] data = new byte[(int)JSONFile.length()];
        fis.read(data);
        fis.close();
        String JSONString = new String(data, "UTF-8");*/
        
        JSONObject jso = JSONObject.parse(fis);
        
        SPLGenerator splGen = new SPLGenerator();
        String SPLString = splGen.generateSPL(jso);
        
        File f = new File(SPLPath);
        PrintWriter splFile = new PrintWriter(f, "UTF-8");
        splFile.print(SPLString);
        splFile.flush();
        splFile.close();
    }
}
