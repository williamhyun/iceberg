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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.PresignRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.ImmutablePresignResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.PresignResponse;

/**
 * Populates the {@code pre-signed-urls} map on completed scan-planning responses, mapping each
 * returned file's immutable, logical location (real S3 locations in practice) to a caller-supplied
 * presigned URL. Manifests and {@code FileScanTask} locations are never rewritten; only the map
 * carries the ephemeral URLs, per the {@code pre-signed-urls} access-delegation design.
 *
 * <p>Also serves {@link #presign} refreshes for a previously-planned set of files, standing in for
 * the catalog's {@code /presign} endpoint. Since {@link RESTCatalogServlet} does not route {@code
 * POST .../presign} automatically, callers need a servlet subclass that intercepts that path and
 * delegates to this method.
 *
 * <p>Lives in {@code org.apache.iceberg.rest} because {@link #handleRequest} overrides a method
 * with a package-private {@code Route} parameter, and is {@code public} so a test in another module
 * can construct it.
 */
public class PresignedUrlVendingAdapter extends RESTCatalogAdapter {

  private final UnaryOperator<String> presign;
  private final long urlLifetimeMs;

  /**
   * @param catalog the backing catalog to delegate ordinary REST operations to
   * @param presign maps a file's stored location to the URL vended to clients; must be stable (same
   *     input yields the same URL) so repeated or concurrent requests stay consistent
   */
  public PresignedUrlVendingAdapter(Catalog catalog, UnaryOperator<String> presign) {
    this(catalog, presign, TimeUnit.HOURS.toMillis(1));
  }

  public PresignedUrlVendingAdapter(
      Catalog catalog, UnaryOperator<String> presign, long urlLifetimeMs) {
    super(catalog);
    this.presign = presign;
    this.urlLifetimeMs = urlLifetimeMs;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends RESTResponse> T handleRequest(
      Route route,
      Map<String, String> vars,
      HTTPRequest httpRequest,
      Class<T> responseType,
      Consumer<Map<String, String>> responseHeaders) {
    T response = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);

    // specsById must be carried over on every rebuilt response: the parsers need it to serialize
    // each file's partition data (accessor deprecated pending a 1.12.0 redesign).
    if (response instanceof PlanTableScanResponse planResponse
        && planResponse.fileScanTasks() != null) {
      return (T)
          PlanTableScanResponse.builder()
              .withPlanStatus(planResponse.planStatus())
              .withPlanId(planResponse.planId())
              .withErrorResponse(planResponse.errorResponse())
              .withCredentials(planResponse.credentials())
              .withSpecsById(planResponse.specsById())
              .withFileScanTasks(planResponse.fileScanTasks())
              .withPreSignedUrls(preSignedUrls(planResponse.fileScanTasks()))
              .withUrlExpirationTimestampMs(urlExpirationTimestampMs())
              .build();
    } else if (response instanceof FetchPlanningResultResponse fetchResponse
        && fetchResponse.fileScanTasks() != null) {
      return (T)
          FetchPlanningResultResponse.builder()
              .withPlanStatus(fetchResponse.planStatus())
              .withErrorResponse(fetchResponse.errorResponse())
              .withPlanTasks(fetchResponse.planTasks())
              .withCredentials(fetchResponse.credentials())
              .withSpecsById(fetchResponse.specsById())
              .withFileScanTasks(fetchResponse.fileScanTasks())
              .withPreSignedUrls(preSignedUrls(fetchResponse.fileScanTasks()))
              .withUrlExpirationTimestampMs(urlExpirationTimestampMs())
              .build();
    }

    return response;
  }

  /** Re-signs the requested file paths, standing in for the catalog's {@code /presign} endpoint. */
  public PresignResponse presign(PresignRequest request) {
    Map<String, String> urls = Maps.newLinkedHashMap();
    for (String path : request.filePaths()) {
      urls.put(path, presign.apply(path));
    }

    return ImmutablePresignResponse.builder()
        .preSignedUrls(urls)
        .urlExpirationTimestampMs(urlExpirationTimestampMs())
        .build();
  }

  private Map<String, String> preSignedUrls(List<FileScanTask> tasks) {
    Map<String, String> urls = Maps.newLinkedHashMap();
    for (FileScanTask task : tasks) {
      urls.put(task.file().location(), presign.apply(task.file().location()));
      for (DeleteFile deleteFile : task.deletes()) {
        urls.put(deleteFile.location(), presign.apply(deleteFile.location()));
      }
    }

    return urls;
  }

  private long urlExpirationTimestampMs() {
    return System.currentTimeMillis() + urlLifetimeMs;
  }
}
