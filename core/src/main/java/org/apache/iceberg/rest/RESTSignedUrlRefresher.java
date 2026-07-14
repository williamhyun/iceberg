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
package org.apache.iceberg.rest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.requests.ImmutablePresignRequest;
import org.apache.iceberg.rest.requests.PresignRequest;
import org.apache.iceberg.rest.responses.PresignResponse;
import org.immutables.value.Value;

/**
 * Re-signs one or more expired pre-signed URLs in a single call to the catalog's {@code /presign}
 * endpoint (see {@link PresignRequest}), keyed by each file's canonical path.
 *
 * <p>Instances are built from a scan-scoped {@code FileIO} property map -- the same map that
 * already carries the catalog {@code uri}, auth ({@code token}/{@code credential}), the {@code
 * plan-id}, and the table's concrete presign path ({@link
 * RESTCatalogProperties#REST_SCAN_PRESIGN_ENDPOINT}), because {@code RESTTableScan} stamps all of
 * these onto the FileIO that is broadcast to executors.
 *
 * <p>This mirrors {@code S3V4RestSignerClient}: the {@link AuthManager} and {@link RESTClient} are
 * shared statically per JVM so that many FileIO instances on one executor reuse a single REST
 * client and auth session rather than each standing up its own. The clients are intentionally
 * long-lived for the JVM, so {@link #close()} is a no-op.
 */
@Value.Immutable
public abstract class RESTSignedUrlRefresher implements AutoCloseable {

  private static final String SCOPE = "sign";

  @SuppressWarnings({"immutables:incompat", "VisibilityModifier"})
  @VisibleForTesting
  static volatile AuthManager authManager;

  @SuppressWarnings({"immutables:incompat", "VisibilityModifier"})
  @VisibleForTesting
  static volatile RESTClient httpClient;

  public abstract Map<String, String> properties();

  @Value.Lazy
  String catalogUri() {
    return properties()
        .getOrDefault(RESTCatalogProperties.SIGNER_URI, properties().get(CatalogProperties.URI));
  }

  @Value.Lazy
  String endpoint() {
    return RESTUtil.resolveEndpoint(
        catalogUri(), properties().get(RESTCatalogProperties.REST_SCAN_PRESIGN_ENDPOINT));
  }

  /**
   * Re-signs {@code path} and returns the fresh pre-signed URL.
   *
   * @param path the file's canonical (logical) storage location
   */
  public String refresh(String path) {
    return refreshAll(List.of(path)).get(path);
  }

  /**
   * Re-signs {@code paths} in a single request and returns a fresh pre-signed URL for each, keyed
   * by the same canonical (logical) storage location passed in.
   *
   * @param paths the files' canonical (logical) storage locations
   */
  public Map<String, String> refreshAll(Collection<String> paths) {
    return presign(paths).preSignedUrls();
  }

  /**
   * Re-signs {@code paths} in a single request and returns the catalog's full response, including
   * the timestamp at which the fresh URLs expire.
   *
   * @param paths the files' canonical (logical) storage locations
   */
  public PresignResponse presign(Collection<String> paths) {
    Preconditions.checkArgument(
        paths != null && !paths.isEmpty(), "Invalid paths to refresh: null or empty");

    PresignRequest request =
        ImmutablePresignRequest.builder()
            .planId(properties().get(RESTCatalogProperties.REST_SCAN_PLAN_ID))
            .filePaths(List.copyOf(paths))
            .build();

    PresignResponse response =
        httpClient()
            .withAuthSession(authSession())
            .post(
                endpoint(),
                request,
                PresignResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

    Map<String, String> freshUrls = response.preSignedUrls();
    for (String path : paths) {
      Preconditions.checkState(
          freshUrls.get(path) != null,
          "Catalog did not return a fresh pre-signed URL for: %s",
          path);
    }

    return response;
  }

  private AuthSession authSession() {
    ImmutableMap.Builder<String, String> sessionProperties =
        ImmutableMap.<String, String>builder()
            .putAll(properties())
            .putAll(OAuth2Util.buildOptionalParam(properties(), SCOPE));

    return authManager().tableSession(httpClient(), sessionProperties.buildKeepingLast());
  }

  private AuthManager authManager() {
    if (null == authManager) {
      synchronized (RESTSignedUrlRefresher.class) {
        if (null == authManager) {
          authManager = AuthManagers.loadAuthManager("http-fileio-refresher", properties());
        }
      }
    }

    return authManager;
  }

  private RESTClient httpClient() {
    if (null == httpClient) {
      synchronized (RESTSignedUrlRefresher.class) {
        if (null == httpClient) {
          // No base URI: the endpoint passed to post() is already fully resolved, and this client
          // may in principle be reused against catalogs other than the one that built it.
          httpClient =
              HTTPClient.builder(properties())
                  .withHeaders(RESTUtil.configHeaders(properties()))
                  .build();
        }
      }
    }

    return httpClient;
  }

  /** No-op: the shared {@link AuthManager} and {@link RESTClient} are long-lived for the JVM. */
  @Override
  public void close() {}

  /**
   * Whether {@code properties} carry enough information (presign endpoint and catalog URI) to
   * refresh a signed URL. Callers should only {@link #create} a refresher when this returns {@code
   * true}.
   */
  public static boolean isConfigured(Map<String, String> properties) {
    String presignPath = properties.get(RESTCatalogProperties.REST_SCAN_PRESIGN_ENDPOINT);
    String catalogUri =
        properties.getOrDefault(
            RESTCatalogProperties.SIGNER_URI, properties.get(CatalogProperties.URI));
    return presignPath != null && catalogUri != null;
  }

  public static RESTSignedUrlRefresher create(Map<String, String> properties) {
    return ImmutableRESTSignedUrlRefresher.builder().properties(properties).build();
  }
}
