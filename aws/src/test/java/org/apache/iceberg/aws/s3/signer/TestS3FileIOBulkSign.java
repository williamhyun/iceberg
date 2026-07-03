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
package org.apache.iceberg.aws.s3.signer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.requests.BatchRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.BatchRemoteSignResponse;
import org.apache.iceberg.rest.responses.ImmutableBatchRemoteSignResponse;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.utils.IoUtils;

/**
 * Verifies that {@link S3FileIO#bulkSign} enumerates a task's files into a single batch sign
 * request with the expected fields (URI, region, provider, plan-id), gates on configuration, and
 * dedups already-signed files. It mocks the signer's static HTTP client to capture the outgoing
 * batch, so it needs no object storage and does not exercise signature validity (a catalog
 * concern).
 *
 * <p>Lives in the signer package because the {@code S3V4RestSignerClient} test hooks it mutates
 * ({@code httpClient}, {@code authManager}) are package-private.
 */
class TestS3FileIOBulkSign {

  private static final String BUCKET = "bulk-sign-bucket";

  private S3Client s3;
  private S3FileIO fileIO;

  @BeforeEach
  void before() {
    S3V4RestSignerClient.authManager = null;
    S3V4RestSignerClient.httpClient = Mockito.mock(RESTClient.class);
    when(S3V4RestSignerClient.httpClient.withAuthSession(any()))
        .thenReturn(S3V4RestSignerClient.httpClient);
    when(S3V4RestSignerClient.httpClient.post(
            anyString(),
            any(RESTRequest.class),
            eq(BatchRemoteSignResponse.class),
            anyMap(),
            any()))
        .thenAnswer(
            invocation -> {
              BatchRemoteSignRequest posted = invocation.getArgument(1);
              List<RemoteSignResponse> signed =
                  posted.requests().stream()
                      .map(
                          request ->
                              (RemoteSignResponse)
                                  ImmutableRemoteSignResponse.builder()
                                      .uri(request.uri())
                                      .headers(Map.of())
                                      .build())
                      .collect(Collectors.toList());
              return ImmutableBatchRemoteSignResponse.builder().responses(signed).build();
            });

    s3 =
        S3Client.builder()
            .region(Region.US_EAST_1)
            .endpointOverride(URI.create("http://localhost:1234"))
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "secret")))
            .build();
  }

  @AfterEach
  void after() {
    if (fileIO != null) {
      fileIO.close();
    }

    IoUtils.closeQuietlyV2(S3V4RestSignerClient.authManager, null);
    S3V4RestSignerClient.authManager = null;
    S3V4RestSignerClient.httpClient = null;
  }

  @Test
  void enumeratesTaskFilesIntoOneBatch() {
    fileIO = fileIO(true, true);

    fileIO.bulkSign(
        List.of("s3://" + BUCKET + "/data.parquet", "s3://" + BUCKET + "/deletes.parquet"));

    ArgumentCaptor<RESTRequest> batch = ArgumentCaptor.forClass(RESTRequest.class);
    verify(S3V4RestSignerClient.httpClient, times(1))
        .post(anyString(), batch.capture(), eq(BatchRemoteSignResponse.class), anyMap(), any());

    List<RemoteSignRequest> requests = ((BatchRemoteSignRequest) batch.getValue()).requests();
    assertThat(requests).hasSize(2);
    assertThat(requests)
        .allSatisfy(
            request -> {
              assertThat(request.method()).isEqualTo("GET");
              assertThat(request.region()).isEqualTo("us-east-1");
              assertThat(request.provider()).isEqualTo("s3");
              assertThat(request.properties())
                  .containsEntry(RESTCatalogProperties.REST_SCAN_PLAN_ID, "plan-123");
            });
    // URIs are the exact object URLs the SDK would resolve for these locations
    assertThat(requests.get(0).uri().toString()).isEqualTo(expectedUrl("data.parquet"));
    assertThat(requests.get(1).uri().toString()).isEqualTo(expectedUrl("deletes.parquet"));
  }

  @Test
  void noRequestWhenBulkSigningDisabled() {
    fileIO = fileIO(true, false);

    fileIO.bulkSign(List.of("s3://" + BUCKET + "/data.parquet"));

    verify(S3V4RestSignerClient.httpClient, never())
        .post(
            anyString(),
            any(RESTRequest.class),
            eq(BatchRemoteSignResponse.class),
            anyMap(),
            any());
  }

  @Test
  void skipsAlreadyCachedFiles() {
    fileIO = fileIO(true, true);
    String data = "s3://" + BUCKET + "/dedup-data.parquet";
    String deletes = "s3://" + BUCKET + "/dedup-deletes.parquet";

    // seed the cache for the data file, then request both
    fileIO.bulkSign(List.of(data));
    fileIO.bulkSign(List.of(data, deletes));

    ArgumentCaptor<RESTRequest> batches = ArgumentCaptor.forClass(RESTRequest.class);
    verify(S3V4RestSignerClient.httpClient, times(2))
        .post(anyString(), batches.capture(), eq(BatchRemoteSignResponse.class), anyMap(), any());

    // assert that the second batch only carries the previously-unsigned delete file
    assertThat(((BatchRemoteSignRequest) batches.getAllValues().get(1)).requests())
        .extracting(request -> request.uri().toString())
        .containsExactly(expectedUrl("dedup-deletes.parquet"));
  }

  @Test
  void bulkSignedFilesAreServedFromCacheOnRead() {
    fileIO = signerBackedFileIO();

    // Emulate a signer server's per-file signing endpoint so a cache miss branch will still
    // complete.
    when(S3V4RestSignerClient.httpClient.post(
            anyString(),
            any(RESTRequest.class),
            eq(RemoteSignResponse.class),
            anyMap(),
            any(),
            any()))
        .thenAnswer(
            invocation -> {
              Consumer<Map<String, String>> responseHeaders = invocation.getArgument(5);
              responseHeaders.accept(Map.of("Cache-Control", "private"));
              RemoteSignRequest request = invocation.getArgument(1);
              return ImmutableRemoteSignResponse.builder()
                  .uri(request.uri())
                  .headers(Map.of())
                  .build();
            });

    fileIO.bulkSign(List.of("s3://" + BUCKET + "/bulk-signed.parquet"));

    // The bulk-signed file's signature is already cached, so reading it should not issue any
    // per-file request.
    attemptGet("bulk-signed.parquet");
    verify(S3V4RestSignerClient.httpClient, never())
        .post(
            anyString(),
            any(RESTRequest.class),
            eq(RemoteSignResponse.class),
            anyMap(),
            any(),
            any());

    // Control: a file that was not bulk-signed falls back to exactly one per-file sign request,
    // confirming reads truly route through the signer and the cache is what removes the round trip.
    attemptGet("not-bulk-signed.parquet");
    verify(S3V4RestSignerClient.httpClient, times(1))
        .post(
            anyString(),
            any(RESTRequest.class),
            eq(RemoteSignResponse.class),
            anyMap(),
            any(),
            any());
  }

  private S3FileIO fileIO(boolean remoteSigningEnabled, boolean bulkEnabled) {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, String.valueOf(remoteSigningEnabled))
            .put(S3FileIOProperties.REMOTE_SIGNING_BULK_ENABLED, String.valueOf(bulkEnabled))
            .put(RESTCatalogProperties.SIGNER_URI, "https://signer.com")
            .put(RESTCatalogProperties.SIGNER_ENDPOINT, "v1/sign/s3")
            .put(RESTCatalogProperties.REST_SCAN_PLAN_ID, "plan-123")
            .build();
    S3FileIO io = new S3FileIO(() -> s3);
    io.initialize(properties);
    return io;
  }

  private S3FileIO signerBackedFileIO() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true")
            .put(S3FileIOProperties.REMOTE_SIGNING_BULK_ENABLED, "true")
            .put(RESTCatalogProperties.SIGNER_URI, "https://signer.com")
            .put(RESTCatalogProperties.SIGNER_ENDPOINT, "v1/sign/s3")
            .put(RESTCatalogProperties.REST_SCAN_PLAN_ID, "plan-123")
            .build();

    // Build the client with the real remote signer installed, exactly as the client factory does
    // in production, so reads exercise genuine SDK request marshalling and the installed signer.
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    S3Client signerClient =
        S3Client.builder()
            .region(Region.US_EAST_1)
            .endpointOverride(URI.create("http://localhost:1234"))
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "secret")))
            .applyMutation(s3Properties::applySignerConfiguration)
            .build();

    S3FileIO io = new S3FileIO(() -> signerClient);
    io.initialize(properties);
    return io;
  }

  private void attemptGet(String key) {
    // No S3 backend is running, so the request fails to connect only after signing is attempted
    // which is the only behavior this test observes.
    assertThatThrownBy(
            () ->
                fileIO
                    .client()
                    .getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()))
        .isInstanceOf(SdkException.class)
        .hasMessageContaining("Unable to execute HTTP request");
  }

  private String expectedUrl(String key) {
    return s3.utilities()
        .getUrl(GetUrlRequest.builder().bucket(BUCKET).key(key).build())
        .toString();
  }
}
