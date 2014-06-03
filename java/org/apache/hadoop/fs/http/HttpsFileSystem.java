package org.apache.hadoop.fs.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Rohini (rohinip@yahoo-inc.com)
 * @version 1.0 Aug 2, 2010
 */
public class HttpsFileSystem extends HttpFileSystem {

  private static final int DEFAULT_PORT = 4443;
  
  @Override
  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }
  
  @Override
  protected String getProtocol() {
    return "https";
  }
  
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    setupSsl(conf);
  }

  /** Set up SSL resources */
  private static void setupSsl(Configuration conf) {
    Configuration sslConf = new Configuration(conf);
    sslConf.addResource(conf.get("dfs.https.client.keystore.resource",
        "ssl-client.xml"));
    System.setProperty("javax.net.ssl.trustStore", sslConf.get(
        "ssl.client.truststore.location", ""));
    System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
        "ssl.client.truststore.password", ""));
    System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
        "ssl.client.truststore.type", "jks"));
    System.setProperty("javax.net.ssl.keyStore", sslConf.get(
        "ssl.client.keystore.location", ""));
    System.setProperty("javax.net.ssl.keyStorePassword", sslConf.get(
        "ssl.client.keystore.password", ""));
    System.setProperty("javax.net.ssl.keyPassword", sslConf.get(
        "ssl.client.keystore.keypassword", ""));
    System.setProperty("javax.net.ssl.keyStoreType", sslConf.get(
        "ssl.client.keystore.type", "jks"));
  }

 
  /**
   * Open an HTTP connection to the namenode to read file data and metadata.
   * @param path The path component of the URL
   * @param query The query component of the URL
   */
  protected HttpURLConnection openConnection(String path, String query, String method)
      throws IOException {
    try {
      final URL url = new URI("https", null, httpServerAddr.getHost(),
          httpServerAddr.getPort(), path, query, null).toURL();
      if (LOG.isTraceEnabled()) {
        LOG.trace("url=" + url);
      }
      HttpsURLConnection connection = (HttpsURLConnection) url.openConnection(httpProxy);   
      connection.setConnectTimeout(connectionTimeout);
      connection.setReadTimeout(readTimeout);
      if (!method.equals(GET_METHOD)) {
        connection.setDoOutput(true);
        connection.setChunkedStreamingMode(chunkSize);
      }
      connection.setDefaultUseCaches(false);
      connection.setRequestMethod(method); 
      connection.setHostnameVerifier(new DummyHostnameVerifier());
      performAuthentication(connection);
      return connection;
    } catch (URISyntaxException e) {
      throw (IOException)new IOException().initCause(e);
    }
  }
  
  /**
   * Dummy hostname verifier that is used to bypass hostname checking
   */
  protected static class DummyHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

}
