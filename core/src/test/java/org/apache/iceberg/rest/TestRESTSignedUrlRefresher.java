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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ImmutablePresignResponse;
import org.apache.iceberg.rest.responses.PresignResponseParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.MediaType;

class TestRESTSignedUrlRefresher {

  private static final String PATH = "s3://bucket/path/file.parquet";
  private static final String FRESH =
      "https://bucket.s3.us-west-2.amazonaws.com/path/file.parquet?sig=fresh";
  private static final String PATH_2 = "s3://bucket/path/file2.parquet";
  private static final String FRESH_2 =
      "https://bucket.s3.us-west-2.amazonaws.com/path/file2.parquet?sig=fresh2";

  private static ClientAndServer mockServer;
  private static String baseUrl;

  @BeforeAll
  static void startServer() {
    mockServer = startClientAndServer(0);
    baseUrl = "http://127.0.0.1:" + mockServer.getPort();
  }

  @AfterAll
  static void stopServer() {
    mockServer.stop();
  }

  @BeforeEach
  void resetServer() {
    mockServer.reset();
  }

  private RESTSignedUrlRefresher refresher;

  @AfterEach
  void closeRefresher() {
    if (refresher != null) {
      refresher.close();
      refresher = null;
    }
  }

  @Test
  void isNotConfiguredWithoutPresignEndpoint() {
    assertThat(RESTSignedUrlRefresher.isConfigured(ImmutableMap.of(CatalogProperties.URI, baseUrl)))
        .isFalse();
  }

  @Test
  void isNotConfiguredWithoutCatalogUri() {
    assertThat(
            RESTSignedUrlRefresher.isConfigured(
                ImmutableMap.of(RESTCatalogProperties.REST_SCAN_PRESIGN_ENDPOINT, "/presign")))
        .isFalse();
  }

  @Test
  void refreshSendsPlanIdAndFilePaths() {
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(PATH, FRESH)));

    refresher = RESTSignedUrlRefresher.create(refresherProperties("plan-abc"));

    assertThat(refresher.refresh(PATH)).isEqualTo(FRESH);

    HttpRequest[] recorded =
        mockServer.retrieveRecordedRequests(request("/presign").withMethod("POST"));
    assertThat(recorded).hasSize(1);
    String body = recorded[0].getBodyAsString();
    assertThat(body).contains("\"plan-id\" : \"plan-abc\"").contains("\"" + PATH + "\"");
  }

  @Test
  void refreshFailsWhenCatalogOmitsRequestedPath() {
    // A catalog that returns pre-signed URLs for a different path cannot satisfy this refresh.
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson("s3://bucket/other/file.parquet", FRESH)));

    refresher = RESTSignedUrlRefresher.create(refresherProperties("plan-abc"));

    assertThatThrownBy(() -> refresher.refresh(PATH))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("did not return a fresh pre-signed URL");
  }

  @Test
  void refreshAllSendsPlanIdAndAllFilePathsInOneRequest() {
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(ImmutableMap.of(PATH, FRESH, PATH_2, FRESH_2))));

    refresher = RESTSignedUrlRefresher.create(refresherProperties("plan-abc"));

    assertThat(refresher.refreshAll(List.of(PATH, PATH_2)))
        .containsEntry(PATH, FRESH)
        .containsEntry(PATH_2, FRESH_2);

    HttpRequest[] recorded =
        mockServer.retrieveRecordedRequests(request("/presign").withMethod("POST"));
    assertThat(recorded).as("all requested paths should be re-signed in a single call").hasSize(1);
    String body = recorded[0].getBodyAsString();
    assertThat(body)
        .contains("\"plan-id\" : \"plan-abc\"")
        .contains("\"" + PATH + "\"")
        .contains("\"" + PATH_2 + "\"");
  }

  @Test
  void refreshAllFailsWhenCatalogOmitsAnyRequestedPath() {
    // The catalog only re-signed one of the two requested paths.
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(ImmutableMap.of(PATH, FRESH))));

    refresher = RESTSignedUrlRefresher.create(refresherProperties("plan-abc"));

    assertThatThrownBy(() -> refresher.refreshAll(List.of(PATH, PATH_2)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("did not return a fresh pre-signed URL")
        .hasMessageContaining(PATH_2);
  }

  @Test
  void refreshAllRejectsEmptyPaths() {
    refresher = RESTSignedUrlRefresher.create(refresherProperties("plan-abc"));

    assertThatThrownBy(() -> refresher.refreshAll(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid paths to refresh: null or empty");
  }

  private Map<String, String> refresherProperties(String planId) {
    return ImmutableMap.of(
        CatalogProperties.URI,
        baseUrl,
        RESTCatalogProperties.REST_SCAN_PRESIGN_ENDPOINT,
        "/presign",
        RESTCatalogProperties.REST_SCAN_PLAN_ID,
        planId);
  }

  private static String presignResponseJson(String path, String freshUrl) {
    return presignResponseJson(ImmutableMap.of(path, freshUrl));
  }

  private static String presignResponseJson(Map<String, String> preSignedUrls) {
    return PresignResponseParser.toJson(
        ImmutablePresignResponse.builder()
            .preSignedUrls(preSignedUrls)
            .urlExpirationTimestampMs(System.currentTimeMillis() + 3_600_000L)
            .build());
  }
}
