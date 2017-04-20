
package com.ibm.streamsx.rest ;

import java.io.IOException ;

import java.net.URL ;
import java.net.MalformedURLException ;

import org.apache.http.client.AuthCache ;
import org.apache.http.impl.client.BasicAuthCache ;
import org.apache.http.impl.auth.BasicScheme ;
import org.apache.http.client.ClientProtocolException ;
import org.apache.http.client.fluent.Executor ;
import org.apache.http.client.fluent.Request ;
import org.apache.http.client.fluent.Response ;

import org.apache.http.HttpHost ;
import org.apache.http.auth.UsernamePasswordCredentials ;
import org.apache.http.auth.Credentials ;
import org.apache.http.auth.AuthScope ;

public class StreamsConnection {

   private Credentials credentials ;
   private HttpHost httpHost ;
   private AuthCache authCache = new BasicAuthCache() ;
   private String url ;

   private Executor executor ;

   public StreamsConnection( String userName, String authToken, String url ) 
   {
      try {
         URL xUrl = new URL( url ) ;
         String hostName = xUrl.getHost() ;

         this.credentials = new UsernamePasswordCredentials( userName, authToken ) ; 
         this.httpHost = new HttpHost( hostName ) ;

         this.authCache.put( httpHost, new BasicScheme() ) ;
 
         this.executor = Executor.newInstance()
           .auth( credentials ) ;
           
         this.url = url ;
      } catch (MalformedURLException e ) {
        e.printStackTrace() ;
     }
   }

   private String getResponseString( String inputString )
   {
      String sReturn = "" ;
      try {
        Request request = Request.Get( inputString )
                          .useExpectContinue() ;
        Response response = executor.execute( request ) ;

        // TODO: need to decode errors
        sReturn = response.returnContent().asString() ;
      }
      catch ( ClientProtocolException e ) {
        e.printStackTrace() ; 
      }
      catch ( IOException e ) {
        e.printStackTrace() ; 
      }
      return sReturn ;
   }

   public static void main( String[] args)
   {
     String userName = args[0] ;
     String authToken = args[1] ;
     String url = args[2] ;
     String inRequest = args[3] ;

     System.out.println( userName ) ;
     System.out.println( authToken ) ;
     System.out.println( url ) ;
     System.out.println( inRequest ) ;
     StreamsConnection sClient = new StreamsConnection( userName, authToken, url ) ;
    
     StringBuilder sRequest = new StringBuilder() ;
     sRequest.append( url ) ;
     sRequest.append( inRequest ) ;

     System.out.println( "Request: " + sRequest.toString()  ) ;
     String sResponse = sClient.getResponseString( sRequest.toString() ) ; 
     System.out.println( sResponse ) ;
   }
}

