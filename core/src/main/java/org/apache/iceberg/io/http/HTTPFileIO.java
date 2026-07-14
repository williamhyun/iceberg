/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io.http;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPreSignedUrls;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTSignedUrlRefresher;
import org.apache.iceberg.rest.responses.PresignResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;

/**
 * A read-only {@link DelegateFileIO} that fetches object bytes via HTTP range GETs, intended for
 * use with pre-signed URLs where the auth is embedded in the URL rather than in credentials held by
 * the reader.
 *
 * <p>{@code location} passed to {@link #newInputFile} is always the file's immutable, logical path;
 * when {@link #setPreSignedUrls} has supplied a pre-signed URL for that path, reads target the URL
 * instead. Without a mapping for a given path, the location itself is read directly, which covers
 * tables whose files are stored at plain, unsigned HTTP(S) locations.
 *
 * <p>Write, delete, and list operations are not supported and will throw {@link
 * UnsupportedOperationException}. The underlying HTTP connection pool is shared across all files
 * opened through this instance.
 *
 * <p>An expired or invalid pre-signed URL (HTTP 403/401) is refreshed once and the read retried,
 * provided this instance's properties carry enough information -- a scan {@code plan-id} and the
 * table's presign path -- for a {@link RESTSignedUrlRefresher} to call back to the catalog. Without
 * that information, an expired URL fails immediately, matching the behavior before refresh support
 * existed.
 *
 * <p>{@link #warmUp} lets a caller proactively refresh the URLs for a set of files it is about to
 * read: once the URLs this instance holds are within {@value #PRESIGN_REFRESH_THRESHOLD_MS} of
 * {@link #urlExpirationTimestampMs()}, the next {@link #warmUp} call re-signs, in one batched
 * request, every location passed to {@link #warmUp} so far. This is a best-effort optimization to
 * avoid a burst of reactive 403s at expiry; the reactive refresh above remains the correctness
 * backstop for files this instance was not asked to warm up.
 *
 * <p>Supported properties:
 *
 * <ul>
 *   <li>{@value #ENABLED} — must be {@code "true"} to activate this FileIO; defaults to {@code
 *       false} so that pre-signed URLs are not silently consumed until explicitly opted in
 *   <li>{@value #MAX_CONNECTIONS} — maximum total pooled connections (default {@value
 *       #DEFAULT_MAX_CONNECTIONS})
 *   <li>{@value #MAX_CONNECTIONS_PER_ROUTE} — maximum pooled connections per host (default {@value
 *       #DEFAULT_MAX_CONNECTIONS_PER_ROUTE})
 *   <li>{@value #CONNECTION_TIMEOUT_MS} — TCP connection timeout in milliseconds
 *   <li>{@value #SOCKET_TIMEOUT_MS} — socket read timeout in milliseconds
 *   <li>{@value #PRESIGN_REFRESH_THRESHOLD_MS} — how long before expiry {@link #warmUp} should
 *       proactively refresh URLs (default {@value #DEFAULT_PRESIGN_REFRESH_THRESHOLD_MS} ms); set
 *       to {@code 0} to disable proactive refresh
 * </ul>
 */
public class HTTPFileIO implements DelegateFileIO, SupportsPreSignedUrls {

  /** Set to {@code "true"} to enable reading via HTTP/HTTPS pre-signed URLs. Default: false. */
  public static final String ENABLED = "http.fileio.enabled";

  public static final String MAX_CONNECTIONS = "http.fileio.max-connections";
  public static final String MAX_CONNECTIONS_PER_ROUTE = "http.fileio.max-connections-per-route";
  public static final String CONNECTION_TIMEOUT_MS = "http.fileio.connection-timeout-ms";
  public static final String SOCKET_TIMEOUT_MS = "http.fileio.socket-timeout-ms";
  public static final String PRESIGN_REFRESH_THRESHOLD_MS =
      "http.fileio.presign-refresh-threshold-ms";

  static final int DEFAULT_MAX_CONNECTIONS = 100;
  static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 100;
  static final long DEFAULT_PRESIGN_REFRESH_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(1);

  private SerializableMap<String, String> properties;
  private volatile SerializableMap<String, String> preSignedUrls = SerializableMap.copyOf(Map.of());
  private volatile long urlExpirationTimestampMs;

  // Not serializable: rebuilt lazily via httpClient()/refresher()/neededPaths() after
  // deserialization, since a FileIO instance is routinely shipped across the wire (e.g. to Spark
  // executors) by engines that serialize the whole Table/FileIO object graph.
  private transient volatile CloseableHttpClient httpClient;
  private transient volatile RESTSignedUrlRefresher refresher;
  private transient volatile boolean refresherResolved;
  private transient volatile Set<String> neededPaths;

  /** No-arg constructor for dynamic loading via {@link org.apache.iceberg.CatalogUtil}. */
  public HTTPFileIO() {}

  @Override
  public void initialize(Map<String, String> props) {
    Preconditions.checkArgument(props != null, "Invalid properties: null");
    Preconditions.checkArgument(
        PropertyUtil.propertyAsBoolean(props, ENABLED, false),
        "HTTPFileIO is disabled. Set %s=true to enable reading via pre-signed HTTP/HTTPS URLs.",
        ENABLED);
    this.properties = SerializableMap.copyOf(props);
  }

  @Override
  public void setPreSignedUrls(Map<String, String> urls, long expirationTimestampMs) {
    this.preSignedUrls = SerializableMap.copyOf(urls != null ? urls : Map.of());
    this.urlExpirationTimestampMs = expirationTimestampMs;
  }

  @Override
  public Map<String, String> preSignedUrls() {
    return preSignedUrls.immutableMap();
  }

  /** The timestamp, as milliseconds since the Unix epoch, when {@link #preSignedUrls()} expire. */
  public long urlExpirationTimestampMs() {
    return urlExpirationTimestampMs;
  }

  @Override
  public void warmUp(Collection<String> locations) {
    if (locations != null && !locations.isEmpty()) {
      neededPaths().addAll(locations);
    }

    refreshIfCloseToExpiry();
  }

  /**
   * Re-signs, in one batched request, every location passed to {@link #warmUp} so far, provided the
   * held URLs are close enough to expiry and a {@link RESTSignedUrlRefresher} is configured.
   * Re-checks both conditions after acquiring the lock so concurrent callers only refresh once.
   */
  private void refreshIfCloseToExpiry() {
    Set<String> paths = neededPaths();
    if (paths.isEmpty() || !closeToExpiry()) {
      return;
    }

    RESTSignedUrlRefresher urlRefresher = refresher();
    if (urlRefresher == null) {
      return;
    }

    synchronized (this) {
      if (!closeToExpiry()) {
        return;
      }

      PresignResponse response = urlRefresher.presign(paths);
      Map<String, String> refreshed = Maps.newHashMap(preSignedUrls.immutableMap());
      refreshed.putAll(response.preSignedUrls());
      this.preSignedUrls = SerializableMap.copyOf(refreshed);
      this.urlExpirationTimestampMs = response.urlExpirationTimestampMs();
    }
  }

  /**
   * Whether {@link #preSignedUrls()} are close enough to {@link #urlExpirationTimestampMs()} that
   * {@link #warmUp} should proactively refresh them, per {@value #PRESIGN_REFRESH_THRESHOLD_MS}.
   */
  private boolean closeToExpiry() {
    long thresholdMs =
        PropertyUtil.propertyAsLong(
            properties(), PRESIGN_REFRESH_THRESHOLD_MS, DEFAULT_PRESIGN_REFRESH_THRESHOLD_MS);
    return thresholdMs > 0
        && urlExpirationTimestampMs > 0
        && System.currentTimeMillis() >= urlExpirationTimestampMs - thresholdMs;
  }

  /** The locations passed to {@link #warmUp} so far by this instance, accumulated across calls. */
  private Set<String> neededPaths() {
    if (neededPaths == null) {
      synchronized (this) {
        if (neededPaths == null) {
          this.neededPaths = ConcurrentHashMap.newKeySet();
        }
      }
    }

    return neededPaths;
  }

  @Override
  public InputFile newInputFile(String location) {
    return new HTTPInputFile(httpClient(), refresher(), location, resolveUrl(location));
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    return new HTTPInputFile(httpClient(), refresher(), location, resolveUrl(location), length);
  }

  /**
   * Resolves {@code location} (a file's immutable, logical path) to the URL a read should target:
   * its pre-signed URL if one was supplied via {@link #setPreSignedUrls}, otherwise the location
   * itself.
   */
  private String resolveUrl(String location) {
    String url = preSignedUrls.get(location);
    return url != null ? url : location;
  }

  @Override
  public OutputFile newOutputFile(String location) {
    throw new UnsupportedOperationException("HTTPFileIO does not support writes");
  }

  @Override
  public void deleteFile(String location) {
    throw new UnsupportedOperationException("HTTPFileIO does not support deletes");
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Map.of() : properties.immutableMap();
  }

  @Override
  public void close() {
    synchronized (this) {
      if (httpClient != null) {
        httpClient.close(CloseMode.GRACEFUL);
        httpClient = null;
      }

      // The refresher shares a static, JVM-lived REST client/auth session, so it is not closed
      // here; dropping the reference is enough.
      refresher = null;
      refresherResolved = false;
      neededPaths = null;
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    throw new UnsupportedOperationException("HTTPFileIO does not support list operations");
  }

  @Override
  public void deletePrefix(String prefix) {
    throw new UnsupportedOperationException("HTTPFileIO does not support delete operations");
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    throw new UnsupportedOperationException("HTTPFileIO does not support delete operations");
  }

  @VisibleForTesting
  CloseableHttpClient httpClient() {
    if (httpClient == null) {
      synchronized (this) {
        if (httpClient == null) {
          Preconditions.checkState(properties != null, "HTTPFileIO is not initialized");
          this.httpClient = buildHttpClient(properties);
        }
      }
    }

    return httpClient;
  }

  /**
   * The refresher used to re-sign expired pre-signed URLs, or {@code null} when this instance's
   * properties do not carry enough information (scan {@code plan-id} and remote-sign path) to
   * refresh. Resolved once and reused; {@code null} is a valid resolved result.
   */
  @VisibleForTesting
  RESTSignedUrlRefresher refresher() {
    if (!refresherResolved) {
      synchronized (this) {
        if (!refresherResolved) {
          Preconditions.checkState(properties != null, "HTTPFileIO is not initialized");
          this.refresher =
              RESTSignedUrlRefresher.isConfigured(properties)
                  ? RESTSignedUrlRefresher.create(properties)
                  : null;
          this.refresherResolved = true;
        }
      }
    }

    return refresher;
  }

  private static CloseableHttpClient buildHttpClient(Map<String, String> props) {
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create();

    connectionManagerBuilder
        .useSystemProperties()
        .setMaxConnTotal(
            PropertyUtil.propertyAsInt(props, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS))
        .setMaxConnPerRoute(
            PropertyUtil.propertyAsInt(
                props, MAX_CONNECTIONS_PER_ROUTE, DEFAULT_MAX_CONNECTIONS_PER_ROUTE));

    ConnectionConfig connectionConfig = buildConnectionConfig(props);
    if (connectionConfig != null) {
      connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);
    }

    return HttpClients.custom().setConnectionManager(connectionManagerBuilder.build()).build();
  }

  private static ConnectionConfig buildConnectionConfig(Map<String, String> props) {
    Long connectionTimeoutMs = PropertyUtil.propertyAsNullableLong(props, CONNECTION_TIMEOUT_MS);
    Integer socketTimeoutMs = PropertyUtil.propertyAsNullableInt(props, SOCKET_TIMEOUT_MS);

    if (connectionTimeoutMs == null && socketTimeoutMs == null) {
      return null;
    }

    ConnectionConfig.Builder connConfigBuilder = ConnectionConfig.custom();

    if (connectionTimeoutMs != null) {
      connConfigBuilder.setConnectTimeout(connectionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    if (socketTimeoutMs != null) {
      connConfigBuilder.setSocketTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS);
    }

    return connConfigBuilder.build();
  }
}
