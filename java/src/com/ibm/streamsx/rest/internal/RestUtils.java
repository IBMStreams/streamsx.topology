/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.internal;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.client.fluent.Executor;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

import com.google.gson.JsonObject;

public interface RestUtils {
    
    static final Logger TRACE = Logger.getLogger("com.ibm.streamsx.rest");

    // V1 credentials members
    String MEMBER_PASSWORD = "password";
    String MEMBER_USERID = "userid";
    
    String AUTH_BEARER = "Bearer ";
    String AUTH_BASIC = "Basic ";

    /**
     * Create an encoded Basic auth header for the given credentials.
     * @param credentials Service credentials.
     * @return the body of a Authentication: Basic header using the user and
     * password contained in the credentials. 
     */
    static String createBasicAuth(JsonObject credentials) {
        return createBasicAuth(jstring(credentials,  MEMBER_USERID),
                jstring(credentials, MEMBER_PASSWORD));
    }

    /**
     * Create an encoded Basic auth header for the given userName and authToken
     * @param userName The user name for authentication
     * @param authToken The password for authentication
     * @return the body of a Authentication: Basic header using the userName
     * and authToken
     */
    static String createBasicAuth(String userName, String authToken) {
        String apiCredentials = userName + ":" + authToken;
        return AUTH_BASIC + DatatypeConverter.printBase64Binary(
                apiCredentials.getBytes(StandardCharsets.UTF_8));
    };

    /**
     * Create an encoded Bearer auth header for the given token.
     * @param tokenBase64 An authentication token, expected to be already
     * encoded in base64, as it is when returned from the IAM server
     * @return the body of a Authentication: Bearer header using tokenBase64
     */
    static String createBearerAuth(String tokenBase64) {
        StringBuilder sb = new StringBuilder(AUTH_BEARER.length()
                + tokenBase64.length());
        sb.append(AUTH_BEARER);
        sb.append(tokenBase64);
        return sb.toString();
    }

    static Executor createExecutor() {
        return Executor.newInstance(createHttpClient(false));
    }

    static Executor createExecutor(boolean allowInsecure) {
        return Executor.newInstance(createHttpClient(allowInsecure));
    }

    static CloseableHttpClient createHttpClient() {
        return createHttpClient(false);
    }

    static CloseableHttpClient createHttpClient(boolean allowInsecure) {
        CloseableHttpClient client = null;
        if (allowInsecure) {
            try {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial(new TrustStrategy() {
                            @Override
                            public boolean isTrusted(X509Certificate[] chain,
                                    String authType) throws CertificateException {
                                return true;
                            }
                        }).build();

                // Set protocols to allow for different handling of "TLS" by Oracle and
                // IBM JVMs.
                SSLConnectionSocketFactory factory =
                        new SSLConnectionSocketFactory(
                                sslContext,
                                new String[] {"TLSv1", "TLSv1.1","TLSv1.2"},
                                null,
                                NoopHostnameVerifier.INSTANCE);
                client = HttpClients.custom()
                        .setSSLSocketFactory(factory)
                        .build();
                TRACE.warning("Insecure host connections enabled.");
            } catch (KeyStoreException | KeyManagementException | NoSuchAlgorithmException e) {
                TRACE.warning("Unable to allow insecure host connections.");
            }
        }
        if (null == client) {
            client = HttpClients.createSystem();
        }
        return client;
    }
}
