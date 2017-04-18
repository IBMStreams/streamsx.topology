
package com.ibm.streamsx.rest ;

import java.io.IOException ;

import org.apache.http.client.ClientProtocolException ;
import org.apache.http.client.fluent.Executor ;
import org.apache.http.client.fluent.Form ;
import org.apache.http.client.fluent.Request ;
import org.apache.http.client.fluent.Response ;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.Gson;

import com.ibm.streamsx.rest.primitives.Instance;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.Operator;
import com.ibm.streamsx.rest.primitives.ActiveVersion;
import com.ibm.streamsx.rest.primitives.Metrics;

public class StreamsConnection {

   private String userName ;
   private String authToken ;
   private String url ;

   private Executor executor ;
//   private CookieStore cookieStore ;

   private static Gson gson ;

   public StreamsConnection( String userName, String authToken, String url ) 
   {
//      this.cookieStore  = new CookieStore() ;
      this.userName = userName ;
      this.authToken = authToken ;
      this.executor = Executor.newInstance()
//         .use( cookieStore )
         .auth( userName, authToken ) ;
      this.url = url ;
      this.gson = new Gson() ;
   }

   private String getResponseString( String inputString ) throws ClientProtocolException, IOException
   {
      String sReturn = "" ;
      try {
        Request request = Request.Get( inputString ) ;
        Response response = executor.execute( request ) ;

        // need to decode errors
        sReturn = response.returnContent().asString() ;
      }
      catch ( ClientProtocolException e ) {
        e.printStackTrace() ; 
      }
      return sReturn ;
   }

   public Instance getInstance( String instanceName )
   {
      StringBuilder sb = new StringBuilder() ; 
      sb.append( url ) ;
      sb.append( "instances/instanceName" ) ;

      String sReturn = "" ;
      try {
        sReturn = getResponseString( sb.toString() ) ;
       } catch ( Exception e ) {
         e.printStackTrace() ;
       }
      // need to check return code
      System.out.println( sReturn ) ;
      Instance si = gson.fromJson( sReturn, Instance.class ) ;

      return si ;
   } 

   public static void main( String[] args)
   {

     String userName = args[0] ;
     String authToken = args[1] ;
     String url = args[2] ;
     String instanceName = args[3] ;

     System.out.println( userName ) ;
     System.out.println( authToken ) ;
     System.out.println( url ) ;
     System.out.println( instanceName ) ;
     StreamsConnection rClient = new StreamsConnection( userName, authToken, url ) ;

     rClient.getInstance( instanceName ) ; 
   }
}

