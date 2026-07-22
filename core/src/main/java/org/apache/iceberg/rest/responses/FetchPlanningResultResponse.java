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
package org.apache.iceberg.rest.responses;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.credentials.Credential;

public class FetchPlanningResultResponse extends BaseScanTaskResponse {
  private final PlanStatus planStatus;
  private final ErrorResponse errorResponse;
  private final List<Credential> credentials;
  private final Map<String, String> preSignedUrls;
  private final Long urlExpirationTimestampMs;

  private FetchPlanningResultResponse(
      PlanStatus planStatus,
      ErrorResponse errorResponse,
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> specsById,
      List<Credential> credentials,
      Map<String, String> preSignedUrls,
      Long urlExpirationTimestampMs) {
    super(planTasks, fileScanTasks, deleteFiles, specsById);
    this.planStatus = planStatus;
    this.errorResponse = errorResponse;
    this.credentials = credentials;
    this.preSignedUrls = preSignedUrls;
    this.urlExpirationTimestampMs = urlExpirationTimestampMs;
    validate();
  }

  public PlanStatus planStatus() {
    return planStatus;
  }

  public ErrorResponse errorResponse() {
    return errorResponse;
  }

  public List<Credential> credentials() {
    return credentials != null ? credentials : ImmutableList.of();
  }

  /**
   * A map of file-path (matching the location of a {@code DataFile} or {@code DeleteFile} returned
   * by this response) to a pre-signed HTTP URL that can be used to read the file's contents
   * directly. Only present when the client requested the {@code pre-signed-urls} access delegation
   * mechanism.
   */
  public Map<String, String> preSignedUrls() {
    return preSignedUrls != null ? preSignedUrls : ImmutableMap.of();
  }

  /**
   * The timestamp, as milliseconds since the Unix epoch, when the URLs in {@link #preSignedUrls()}
   * expire. Present exactly when {@link #preSignedUrls()} is non-empty.
   */
  public Long urlExpirationTimestampMs() {
    return urlExpirationTimestampMs;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(planStatus() != null, "Invalid status: null");
    Preconditions.checkArgument(
        planStatus() == PlanStatus.COMPLETED || (planTasks() == null && fileScanTasks() == null),
        "Invalid response: tasks can only be returned in a 'completed' status");
    Preconditions.checkArgument(
        planStatus() == PlanStatus.FAILED || errorResponse() == null,
        "Invalid response: error can only be returned in a 'failed' status");
    if (fileScanTasks() == null || fileScanTasks().isEmpty()) {
      Preconditions.checkArgument(
          (deleteFiles() == null || deleteFiles().isEmpty()),
          "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
    }
    Preconditions.checkArgument(
        preSignedUrls().isEmpty() == (urlExpirationTimestampMs() == null),
        "Invalid response: urlExpirationTimestampMs must be set if and only if preSignedUrls is present");
  }

  public static class Builder
      extends BaseScanTaskResponse.Builder<Builder, FetchPlanningResultResponse> {
    private Builder() {}

    private PlanStatus planStatus;
    private ErrorResponse errorResponse;
    private final List<Credential> credentials = Lists.newArrayList();
    private final Map<String, String> preSignedUrls = Maps.newHashMap();
    private Long urlExpirationTimestampMs;

    public Builder withPlanStatus(PlanStatus status) {
      this.planStatus = status;
      return this;
    }

    public Builder withErrorResponse(ErrorResponse response) {
      this.errorResponse = response;
      return this;
    }

    public Builder withCredentials(List<Credential> credentialsToAdd) {
      credentials.addAll(credentialsToAdd);
      return this;
    }

    public Builder withPreSignedUrls(Map<String, String> preSignedUrlsToAdd) {
      preSignedUrls.putAll(preSignedUrlsToAdd);
      return this;
    }

    public Builder withUrlExpirationTimestampMs(Long timestampMs) {
      this.urlExpirationTimestampMs = timestampMs;
      return this;
    }

    @Override
    public FetchPlanningResultResponse build() {
      return new FetchPlanningResultResponse(
          planStatus,
          errorResponse,
          planTasks(),
          fileScanTasks(),
          deleteFiles(),
          specsById(),
          credentials,
          preSignedUrls,
          urlExpirationTimestampMs);
    }
  }
}
