package org.apache.hadoop.fs.http;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.http.authentication.client.HttpAuthenticator;
import org.apache.hadoop.http.authentication.client.kerberos.KerberosAuthenticator;
import org.apache.hadoop.http.exception.HttpRemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Rohini (rohinip@yahoo-inc.com)
 * @version 1.0 Aug 2, 2010
 */
public class HttpFileSystem extends FileSystem {

  public static final String HTTP_TIMEZONE = "UTC";
  public static final String HTTP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
  public static final String CONF_HTTP_CONNECTION_TIMEOUT = "http.connection.timeout";
  public static final String CONF_HTTP_READ_TIMEOUT = "http.read.timeout";
  public static final String CONF_HTTP_CHUNK_SIZE = "http.chunk.size";
  public static final String CONF_HTTP_PROXY_HOST = "http.proxy.host";
  public static final String CONF_HTTP_PROXY_PORT = "http.proxy.port";
  public static final String AUTHENTICATION_IMPL = "http.authentication.impl";
  public static final long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;

  private static final int DEFAULT_PORT = 8080;
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String DEFAULT_AUTH = "Default";
  protected static final String GET_METHOD = "GET";
  protected static final String PUT_METHOD = "PUT";
  protected static final String POST_METHOD = "POST";

  protected int readTimeout;
  protected int connectionTimeout;
  protected int chunkSize;

  protected URI httpServerAddr;
  protected Proxy httpProxy = Proxy.NO_PROXY;
  private HttpAuthenticator authenticator = null;
  private final Map<String, HttpAuthenticator> authCache = new ConcurrentHashMap<String, HttpAuthenticator>();
  private final Map<String, String> authConf = new ConcurrentHashMap<String, String>();
  private UserGroupInformation ugi;
  private Path workingDir;

  private static final ThreadLocal<HttpFSThreadLocals> threadLocals = new ThreadLocal<HttpFSThreadLocals>() {
    protected HttpFSThreadLocals initialValue() {
      return new HttpFSThreadLocals();
    }
  };

  private static class HttpFSThreadLocals {
    private SAXParser saxParser = null;
    private SimpleDateFormat dateFormat = null;

    public SAXParser getSaxParser() {
      if (saxParser == null) {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        try {
          saxParser = spf.newSAXParser();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return saxParser;
    }

    public SimpleDateFormat getDateFormat() {
      if (dateFormat == null) {
        dateFormat = new SimpleDateFormat(HTTP_DATE_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone(HTTP_TIMEZONE));
      }
      return dateFormat;
    }
  }

  @Override
  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }

  @Override
  public String getCanonicalServiceName() {
    return httpServerAddr.toString();
  }

  protected String getProtocol() {
    return "http";
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    int port = (name.getPort() == -1) ? getDefaultPort() : name.getPort();
    try {
      httpServerAddr = new URI(getProtocol(), null, name.getHost(), port, name.getPath(), null,
          null);
      ugi = UserGroupInformation.getCurrentUser();
      workingDir = getHomeDirectory();
      connectionTimeout = conf.getInt(CONF_HTTP_CONNECTION_TIMEOUT, 300000);
      readTimeout = conf.getInt(CONF_HTTP_READ_TIMEOUT, 300000);
      chunkSize = conf.getInt(CONF_HTTP_CHUNK_SIZE, 16384);
      String proxyHost = conf.get(CONF_HTTP_PROXY_HOST);
      String proxyPort = conf.get(CONF_HTTP_PROXY_PORT);
      if (proxyHost != null && proxyPort != null) {
        InetSocketAddress addr = new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort));
        httpProxy = new Proxy(Type.HTTP, addr);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    StringBuilder query = new StringBuilder();
    query.append("op=append");
    query.append("&bufferSize=").append(bufferSize);
    HttpURLConnection connection = openConnection(getURLPath(f), query.toString(), PUT_METHOD);
    connection.setRequestProperty("Content-Type", "application/octet-stream");
    return new FSDataOutputStream(new HttpConnectionOutputStream(connection), statistics);
  }

  class HttpConnectionOutputStream extends OutputStream {

    private HttpURLConnection connection;
    private OutputStream out;
    
    public HttpConnectionOutputStream(HttpURLConnection connection) throws IOException {
      this.connection = connection;
      this.out = connection.getOutputStream();
    }
    
    @Override
    public void close() throws IOException {
      out.close();
      validateStatusCode(connection);
      releaseConnection(connection);
    }
    
    @Override
    public void flush() throws IOException {
      out.flush();
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }
    
    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

  }
  
  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    return create(f, null, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    StringBuilder query = new StringBuilder();
    query.append("op=create");
    query.append(overwrite ? "&overwrite=true" : "");
    query.append("&bufferSize=").append(bufferSize);
    query.append("&replication=").append(replication);
    query.append("&blockSize=").append(blockSize);
    if (permission != null) {
      query.append("&permission=").append(permission.getUserAction().ordinal())
          .append(permission.getGroupAction().ordinal())
          .append(permission.getOtherAction().ordinal());
    }
    HttpURLConnection connection = openConnection(getURLPath(f), query.toString(), PUT_METHOD);
    connection.setRequestProperty("Content-Type", "application/octet-stream");
    return new FSDataOutputStream(new HttpConnectionOutputStream(connection), statistics);
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
  }

  @Override
  public short getDefaultReplication() {
    return (short) getConf().getInt("dfs.replication", 3);
  }

  @Override
  public boolean delete(Path f) throws IOException {
    return delete(f, false);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    String skipTrash = getConf().getLong("http.fs.trash.interval", 60) == 0 ? "&skipTrash=true" : "";
    HttpURLConnection connection = openConnection(getURLPath(f), "op=delete" +
        (recursive ? "&recursive=true" : "") + skipTrash, PUT_METHOD);
    try {
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
    return true;
  }

  @Override
  public URI getUri() {
    return httpServerAddr;
  }

  @Override
  public Path getHomeDirectory() {
    return new Path("/user/" + (ugi.getShortUserName()));
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  @Override
  public void setWorkingDirectory(Path newDir) {
    this.workingDir = newDir;
  }

  /** Class to parse and store a listing reply from the server. */
  private class LsParser extends DefaultHandler {

    private final ArrayList<FileStatus> fslist = new ArrayList<FileStatus>();
    private final SimpleDateFormat ldf;
    private final SAXParser parser;

    public LsParser() {
      HttpFSThreadLocals threadLocal = threadLocals.get();
      ldf = threadLocal.getDateFormat();
      parser = threadLocal.getSaxParser();
    }

    public void startElement(String ns, String localname, String qname, Attributes attrs)
        throws SAXException {
      if ("listing".equals(qname))
        return;
      if (!"file".equals(qname) && !"directory".equals(qname)) {
        if (HttpRemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(HttpRemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }
      long modif;
      long atime = 0;
      try {
        modif = ldf.parse(attrs.getValue("modified")).getTime();
        String astr = attrs.getValue("accesstime");
        if (astr != null) {
          atime = ldf.parse(astr).getTime();
        }
      } catch (ParseException e) {
        throw new SAXException(e);
      }
      FileStatus fs = "file".equals(qname)
        ? new FileStatus(
              Long.valueOf(attrs.getValue("size")), false,
              Short.valueOf(attrs.getValue("replication")),
              Long.valueOf(attrs.getValue("blocksize")),
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path(getUri().toString(), attrs.getValue("path"))
                .makeQualified(HttpFileSystem.this))
        : new FileStatus(0L, true, 0, 0L,
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path(getUri().toString(), attrs.getValue("path"))
                .makeQualified(HttpFileSystem.this));
      fslist.add(fs);
    }

    private void fetchList(Path p, boolean recur) throws IOException {
      HttpURLConnection connection = openConnection(getURLPath(p), "op=status" +
          (recur ? "&recursive=yes" : ""), GET_METHOD);
      try {
        connection.connect();
        validateStatusCode(connection);
        parser.parse(connection.getInputStream(), this);
        parser.reset();
      } catch (SAXException e) {
        final Exception embedded = e.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException) embedded;
        }
        throw new IOException("invalid xml directory content", e);
      } finally {
        releaseConnection(connection);
      }
    }

    public FileStatus getFileStatus(Path f) throws IOException {
      fetchList(f, false);
      if (fslist.size() == 0) {
        throw new FileNotFoundException("File does not exist: " + f);
      }
      return fslist.get(0);
    }

    public FileStatus[] listStatus(Path f, boolean recur) throws IOException {
      fetchList(f, recur);
      int size = fslist.size();
      if (fslist.size() > 0 && (fslist.size() != 1 || fslist.get(0).isDir())) {
        fslist.remove(0);
        size = size - 1;
      }
      return fslist.toArray(new FileStatus[size]);
    }

    public FileStatus[] listStatus(Path f) throws IOException {
      return listStatus(f, false);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.getFileStatus(f);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.listStatus(f);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    HttpURLConnection connection = openConnection(getURLPath(f), "op=mkdir",
        PUT_METHOD);
    try {
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
    return true;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    String perms = "" + permission.getUserAction().ordinal() +
        permission.getGroupAction().ordinal() +
        permission.getOtherAction().ordinal();
    HttpURLConnection connection = openConnection(getURLPath(f), "op=mkdir&permission=" + perms,
        PUT_METHOD);
    try {
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
    return true;
  }

  private static class HttpFSInputStream extends FSInputStream {
    private final InputStream in;
    private final long filelength;
    long currentPos = 0;

    private HttpFSInputStream(InputStream in, long filelength) {
      this.in = in;
      this.filelength = filelength;
    }

    private void update(final boolean isEOF, final int n) throws IOException {
      if (!isEOF) {
        currentPos += n;
      } else if (currentPos < filelength) {
        throw new IOException("Got EOF but byteread = " + currentPos + " < filelength = " +
            filelength);
      }
    }

    public int read() throws IOException {
      final int b = in.read();
      update(b == -1, 1);
      return b;
    }

    public int read(byte[] b, int off, int len) throws IOException {
      final int n = in.read(b, off, len);
      update(n == -1, n);
      return n;
    }

    public void close() throws IOException {
      in.close();
    }

    public void seek(long pos) throws IOException {
      throw new IOException("Can't seek!");
    }

    public long getPos() throws IOException {
      return currentPos;
    }

    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    HttpURLConnection connection = openConnection(getURLPath(f), null, GET_METHOD);
    connection.connect();
    validateStatusCode(connection);
    final String cl = connection.getHeaderField(CONTENT_LENGTH);
    final long filelength = cl == null ? -1 : Long.parseLong(cl);
    if (LOG.isDebugEnabled()) {
      LOG.debug("filelength = " + filelength);
    }
    final InputStream in = connection.getInputStream();
    return new FSDataInputStream(new HttpFSInputStream(in, filelength));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    HttpURLConnection connection = null;
    try {
      connection = openConnection(getURLPath(src), "op=move&dest=" + getAbsolutePath(dst),
          PUT_METHOD);
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
    return true;
  }

  /**
   * A parser for parsing {@link ContentSummary} xml.
   */
  private class ContentSummaryParser extends DefaultHandler {
    private ContentSummary contentsummary;

    /** {@inheritDoc} */
    public void startElement(String ns, String localname, String qname, Attributes attrs)
        throws SAXException {
      if (!ContentSummary.class.getName().equals(qname)) {
        throw new SAXException("Unrecognized entry: " + qname);
      }

      contentsummary = toContentSummary(attrs);
    }

    /**
     * Connect to the name node and get content summary.
     * 
     * @param path The path
     * @return The content summary for the path.
     * @throws IOException
     */
    private ContentSummary getContentSummary(Path p) throws IOException {
      final HttpURLConnection connection = openConnection(getURLPath(p), "op=contentSummary",
          GET_METHOD);
      connection.connect();
      validateStatusCode(connection);
      InputStream in = null;
      try {
        in = connection.getInputStream();
        SAXParser parser = threadLocals.get().getSaxParser();
        parser.parse(in, this);
        parser.reset();
      } catch (FileNotFoundException ignore) {
        // the server may not support getContentSummary
        return null;
      } catch (SAXException saxe) {
        final Exception embedded = saxe.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException) embedded;
        }
        throw new IOException("Invalid xml format", saxe);
      } finally {
        if (in != null) {
          in.close();
        }
        releaseConnection(connection);
      }
      return contentsummary;
    }
  }

  // Return the object represented in the attributes.
  private static ContentSummary toContentSummary(Attributes attrs) throws SAXException {
    final String length = attrs.getValue("length");
    final String fileCount = attrs.getValue("fileCount");
    final String directoryCount = attrs.getValue("directoryCount");
    final String quota = attrs.getValue("quota");
    final String spaceConsumed = attrs.getValue("spaceConsumed");
    final String spaceQuota = attrs.getValue("spaceQuota");

    if (length == null || fileCount == null || directoryCount == null || quota == null ||
        spaceConsumed == null || spaceQuota == null) {
      return null;
    }

    try {
      return new ContentSummary(
          Long.parseLong(length),
          Long.parseLong(fileCount),
          Long.parseLong(directoryCount),
          Long.parseLong(quota),
          Long.parseLong(spaceConsumed),
          Long.parseLong(spaceQuota));
    } catch(Exception e) {
      throw new SAXException("Invalid attributes: length=" + length +
          ", fileCount=" + fileCount +
          ", directoryCount=" + directoryCount +
          ", quota=" + quota +
          ", spaceConsumed=" + spaceConsumed +
          ", spaceQuota=" + spaceQuota, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    final ContentSummary cs = new ContentSummaryParser().getContentSummary(f);
    return cs != null ? cs : super.getContentSummary(f);
  }

  private class ChecksumParser extends DefaultHandler {
    private FileChecksum filechecksum;

    /** {@inheritDoc} */
    public void startElement(String ns, String localname, String qname, Attributes attrs)
        throws SAXException {
      if (!MD5MD5CRC32FileChecksum.class.getName().equals(qname)) {
        throw new SAXException("Unrecognized entry: " + qname);
      }

      filechecksum = MD5MD5CRC32FileChecksum.valueOf(attrs);
    }

    private FileChecksum getFileChecksum(Path p) throws IOException {
      HttpURLConnection connection = openConnection(getURLPath(p), "op=fileChecksum", GET_METHOD);

      try {
        connection.connect();
        validateStatusCode(connection);
        SAXParser parser = threadLocals.get().getSaxParser();
        parser.parse(connection.getInputStream(), this);
        parser.reset();
      } catch (SAXException e) {
        final Exception embedded = e.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException) embedded;
        }
        throw new IOException("invalid xml directory content", e);
      } finally {
        releaseConnection(connection);
      }
      return filechecksum;
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return new ChecksumParser().getFileChecksum(f);
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    setOwner(p, username, groupname, false);
  }
  
  public void setOwner(Path p, String username, String groupname, boolean recursive)
      throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    
    String query = "";
    if (username != null) {
      query += "&owner=" + username;
    }
    if (groupname != null)
      query += "&group=" + groupname;
    query += recursive ? "&recursive=true" : "";
    HttpURLConnection connection = openConnection(getURLPath(p), "op=chown" + query, PUT_METHOD);
    try {
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    setPermission(p, permission, false);
  }
  
  public void setPermission(Path p, FsPermission permission, boolean recursive) throws IOException {
    String mode = "" + permission.getUserAction().ordinal() +
        permission.getGroupAction().ordinal() +
        permission.getOtherAction().ordinal();
    HttpURLConnection connection = openConnection(getURLPath(p),
        "op=chmod&permission=" + mode + (recursive ? "&recursive=true" : ""),
        PUT_METHOD);
    try {
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    HttpURLConnection connection = openConnection(getURLPath(new Path("/")), "op=close", PUT_METHOD);
    try {
      connection.connect();
      validateStatusCode(connection);
    } finally {
      releaseConnection(connection);
    }
  }

  private String getURLPath(Path p) {
    return "/fs" + getAbsolutePath(p);
  }

  private String getAbsolutePath(Path p) {
    Path absolutePath = p;
    if (!p.isAbsolute()) {
      absolutePath = new Path(workingDir, p);
    }
    return absolutePath.toUri().getPath();
  }

  /**
   * Open an HTTP connection to the namenode to read file data and metadata.
   * 
   * @param path
   *          The path component of the URL
   * @param query
   *          The query component of the URL
   */
  protected HttpURLConnection openConnection(String path, String query, String method)
      throws IOException {
    try {
      final URL url = new URI("http", null, httpServerAddr.getHost(), httpServerAddr.getPort(),
          path, query, null).toURL();
      if (LOG.isTraceEnabled()) {
        LOG.trace("url=" + url);
      }
      HttpURLConnection connection = (HttpURLConnection) url.openConnection(httpProxy);
      connection.setConnectTimeout(connectionTimeout);
      connection.setReadTimeout(readTimeout);
      if (!method.equals(GET_METHOD)) {
        connection.setDoOutput(true);
        connection.setChunkedStreamingMode(chunkSize);
      }
      connection.setDefaultUseCaches(false);
      connection.setRequestMethod(method);
      performAuthentication(connection);
      return connection;
    } catch (URISyntaxException e) {
      throw (IOException) new IOException().initCause(e);
    }
  }

  private void releaseConnection(HttpURLConnection connection) {
    try {
      if (connection != null)
        connection.disconnect();
    } catch (Throwable t) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error while closing connection", t);
      }
    }
  }

  protected void performAuthentication(HttpURLConnection connection) throws IOException {
    String renewAuth = getConf().get(HttpAuthenticator.CONF_FOR_HTTP_RENEW_AUTHENTICATION);
    if (renewAuth != null) {
      reloadAuthConf();
    }
    getHttpAuthenticator().authenticate(authConf, connection);
  }

  private void reloadAuthConf() {
    authConf.clear();
    for (Map.Entry<String, String> entry : getConf()) {
      authConf.put(entry.getKey(), entry.getValue());
    }
  }

  private HttpAuthenticator getHttpAuthenticator() throws IOException {
    if (authenticator == null) {
      String authMechanism = getConf().get(AUTHENTICATION_IMPL);
      if (authMechanism == null) {
        authenticator = authCache.get(DEFAULT_AUTH);
        if (authenticator == null) {
          authenticator = new KerberosAuthenticator();
          authCache.put(DEFAULT_AUTH, authenticator);
        }
      } else {
        authenticator = authCache.get(authMechanism);
        if (authenticator == null) {
          authenticator = (HttpAuthenticator) getNewInstance(authMechanism);
          authCache.put(authMechanism, authenticator);
        }
      }
      reloadAuthConf();
    }
    return authenticator;
  }

  private Object getNewInstance(String className) {
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> constructor = clazz.getConstructor();
      return constructor.newInstance();
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unable to access instance of " + className, e);
    }
  }

  private void validateStatusCode(HttpURLConnection connection) throws IOException {
    int statusCode = connection.getResponseCode();
    handleCookiesInResponse(connection);
    String errorMsg = null;
    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP Status Code=" + statusCode);
    }
    if (((statusCode >= 400 && statusCode < 600) || statusCode == 301)) {
      if (statusCode == 401) {
        getHttpAuthenticator().clearCookie();
      }
      try {
        ExceptionHandler exceptionHandler = new ExceptionHandler();
        InputStream is = connection.getErrorStream();
        if (is == null) {
          throw new IOException("Server returned status code: " + statusCode);
        }
        byte[] error = new byte[is.available()];
        is.read(error);
        errorMsg = new String(error).trim();
        errorMsg = StringEscapeUtils.unescapeXml(errorMsg);
        SAXParser parser = threadLocals.get().getSaxParser();
        parser.parse(new ByteArrayInputStream(errorMsg.getBytes()), exceptionHandler);
        parser.reset();

        if (exceptionHandler.getRemoteException() != null) {
          HttpRemoteException exception = exceptionHandler.getRemoteException();
          throw exception.unwrapRemoteException();
        }
      } catch (IOException e) {
        throw e;
      } catch (SAXException ignore) {
        throw new IOException(errorMsg);
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }

  private void handleCookiesInResponse(HttpURLConnection connection) throws IOException {
    getHttpAuthenticator().setCookieFromResponse(connection);
  }

  private static class ExceptionHandler extends DefaultHandler {

    private HttpRemoteException exception = null;

    public HttpRemoteException getRemoteException() {
      return exception;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
        throws SAXException {
      if (HttpRemoteException.class.getSimpleName().equals(qName)) {
        exception = HttpRemoteException.valueOf(attributes);
      }
    }

  }
}
