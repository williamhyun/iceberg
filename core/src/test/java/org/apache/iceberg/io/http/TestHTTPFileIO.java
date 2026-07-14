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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.responses.ImmutablePresignResponse;
import org.apache.iceberg.rest.responses.PresignResponseParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.MediaType;
import org.mockserver.verify.VerificationTimes;

class TestHTTPFileIO {

  private static ClientAndServer mockServer;
  private static String baseUrl;
  private static HTTPFileIO fileIO;

  @BeforeAll
  static void startServer() {
    mockServer = startClientAndServer(0);
    baseUrl = "http://127.0.0.1:" + mockServer.getPort();
    fileIO = new HTTPFileIO();
    fileIO.initialize(ImmutableMap.of(HTTPFileIO.ENABLED, "true"));
  }

  @AfterAll
  static void stopServer() {
    fileIO.close();
    mockServer.stop();
  }

  private HTTPFileIO refreshableFileIO;

  @BeforeEach
  void resetServer() {
    mockServer.reset();
  }

  @AfterEach
  void closeRefreshableFileIO() {
    if (refreshableFileIO != null) {
      refreshableFileIO.close();
      refreshableFileIO = null;
    }
  }

  // ---- initialization ----

  @Test
  void disabledByDefault() {
    HTTPFileIO disabled = new HTTPFileIO();
    assertThatThrownBy(() -> disabled.initialize(ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(HTTPFileIO.ENABLED);
  }

  @Test
  void propertiesReturnsInitializedProperties() {
    assertThat(fileIO.properties()).containsEntry(HTTPFileIO.ENABLED, "true");
  }

  @Test
  void survivesJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    // Spark ships a FileIO to executors via serialization. The non-serializable HTTP client must be
    // excluded and rebuilt lazily on the deserialized copy.
    byte[] data = new byte[] {1, 2, 3};
    String path = "/roundtrip.bin";
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-2"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    HTTPFileIO deserialized = roundTripSerialize(fileIO);

    assertThat(deserialized.properties()).containsEntry(HTTPFileIO.ENABLED, "true");

    byte[] buffer = new byte[3];
    try (SeekableInputStream stream = deserialized.newInputFile(baseUrl + path).newStream()) {
      ((RangeReadable) stream).readFully(0, buffer, 0, 3);
    }

    assertThat(buffer).isEqualTo(data);
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTripSerialize(T obj) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(obj);
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  // ---- getLength ----

  @Test
  void getLengthReturnsKnownLengthWithoutRemoteRequest() {
    InputFile inputFile = fileIO.newInputFile(baseUrl + "/file.parquet", 42_000L);
    assertThat(inputFile.getLength()).isEqualTo(42_000L);
    // No request should have been issued to the server.
    mockServer.verifyZeroInteractions();
  }

  @Test
  void getLengthIssuesZeroRangeGetWhenUnknown() {
    String path = "/file.parquet";
    // GET Range: bytes=0-0 → 206 with Content-Range: bytes 0-0/12345
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-0"))
        .respond(
            response()
                .withStatusCode(206)
                .withHeader("Content-Range", "bytes 0-0/12345")
                .withBody(new byte[] {0}));

    InputFile inputFile = fileIO.newInputFile(baseUrl + path);
    assertThat(inputFile.getLength()).isEqualTo(12_345L);
  }

  @Test
  void getLengthFallsBackToContentLengthOn200() {
    // A server that does not support range requests returns 200 with the full body.
    byte[] body = new byte[99];
    String path = "/no-range.parquet";
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-0"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(body));

    assertThat(fileIO.newInputFile(baseUrl + path).getLength()).isEqualTo(99L);
  }

  @Test
  void getLengthThrowsNotFoundOn404() {
    String path = "/missing.parquet";
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-0"))
        .respond(response().withStatusCode(404));

    InputFile inputFile = fileIO.newInputFile(baseUrl + path);
    assertThatThrownBy(inputFile::getLength)
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("missing.parquet");
  }

  // ---- exists ----

  @Test
  void existsReturnsTrueOn206() {
    String path = "/present.parquet";
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-0"))
        .respond(
            response()
                .withStatusCode(206)
                .withHeader("Content-Range", "bytes 0-0/100")
                .withBody(new byte[] {0}));

    assertThat(fileIO.newInputFile(baseUrl + path).exists()).isTrue();
  }

  @Test
  void existsReturnsFalseOnNon2xx() {
    String path = "/absent.parquet";
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-0"))
        .respond(response().withStatusCode(404));

    assertThat(fileIO.newInputFile(baseUrl + path).exists()).isFalse();
  }

  // ---- readFully ----

  @Test
  void readFullySendsCorrectRangeHeader() throws IOException {
    byte[] content = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    // "llo, " = bytes[2..6]
    byte[] slice = Arrays.copyOfRange(content, 2, 7);
    String path = "/data.bin";

    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=2-6"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(slice));

    byte[] buffer = new byte[5];
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      ((RangeReadable) stream).readFully(2, buffer, 0, 5);
    }

    assertThat(buffer).isEqualTo(slice);
  }

  @Test
  void readFullyAcceptsHttp200AsWellAs206() throws IOException {
    byte[] data = new byte[] {10, 20, 30};
    String path = "/full.bin";

    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-2"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    byte[] buffer = new byte[3];
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      ((RangeReadable) stream).readFully(0, buffer, 0, 3);
    }

    assertThat(buffer).isEqualTo(data);
  }

  // ---- readTail ----

  @Test
  void readTailSendsSuffixRangeHeader() throws IOException {
    byte[] footer = new byte[] {1, 2, 3, 4};
    String path = "/file.parquet";

    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=-4"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(footer));

    byte[] buffer = new byte[4];
    int read;
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      read = ((RangeReadable) stream).readTail(buffer, 0, 4);
    }

    assertThat(read).isEqualTo(4);
    assertThat(buffer).isEqualTo(footer);
  }

  @Test
  void readTailReturnsActualBytesWhenServerReturnsLess() throws IOException {
    byte[] shortFooter = new byte[] {7, 8};
    String path = "/short.parquet";

    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=-10"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(shortFooter));

    byte[] buffer = new byte[10];
    int read;
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      read = ((RangeReadable) stream).readTail(buffer, 0, 10);
    }

    assertThat(read).isEqualTo(2);
    assertThat(Arrays.copyOf(buffer, 2)).isEqualTo(shortFooter);
  }

  // ---- sequential read and seek ----

  @Test
  void sequentialReadBuffersAndServesChunk() throws IOException {
    byte[] data = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
    String path = "/seq.txt";

    // The stream fetches a chunk starting at offset 0
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withHeader("Range", String.format("bytes=0-%d", HTTPInputStream.CHUNK_SIZE - 1)))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    byte[] buf = new byte[data.length];
    int totalRead = 0;
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      int bytesRead;
      while ((bytesRead = stream.read(buf, totalRead, buf.length - totalRead)) > 0) {
        totalRead += bytesRead;
      }
    }

    assertThat(totalRead).isEqualTo(data.length);
    assertThat(buf).isEqualTo(data);
  }

  @Test
  void seekUpdatesPositionAndFetchesNewChunk() throws IOException {
    byte[] full = "0123456789".getBytes(StandardCharsets.UTF_8);
    String path = "/seek.txt";

    // After seek(5), the stream fetches bytes starting at 5.
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withHeader(
                    "Range", String.format("bytes=5-%d", 5 + HTTPInputStream.CHUNK_SIZE - 1)))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(Arrays.copyOfRange(full, 5, full.length)));

    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      stream.seek(5);
      assertThat(stream.getPos()).isEqualTo(5L);

      int byteVal = stream.read();
      assertThat((char) byteVal).isEqualTo('5');
      assertThat(stream.getPos()).isEqualTo(6L);
    }
  }

  @Test
  void seekBeforeReadDoesNotIssueRequest() throws IOException {
    // A bare seek() with no subsequent read should make no HTTP request.
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + "/seekonly.bin").newStream()) {
      stream.seek(1024);
      assertThat(stream.getPos()).isEqualTo(1024L);
    }

    mockServer.verifyZeroInteractions();
  }

  @Test
  void rangeNotSatisfiableAtEofIsTreatedAsEndOfStream() throws IOException {
    byte[] data = "ABCDE".getBytes(StandardCharsets.UTF_8);
    String path = "/eof.bin";

    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withHeader("Range", String.format("bytes=0-%d", HTTPInputStream.CHUNK_SIZE - 1)))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    // A read at EOF asks for a range past the file's end; S3 answers 416 rather than an empty 206.
    // This must be treated as end of stream, not a failure, since callers (e.g. Avro) probe at EOF.
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withHeader(
                    "Range",
                    String.format(
                        "bytes=%d-%d", data.length, data.length + HTTPInputStream.CHUNK_SIZE - 1)))
        .respond(response().withStatusCode(416));

    byte[] buf = new byte[data.length];
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      int totalRead = 0;
      int bytesRead;
      while ((bytesRead = stream.read(buf, totalRead, buf.length - totalRead)) > 0) {
        totalRead += bytesRead;
      }

      assertThat(totalRead).isEqualTo(data.length);
      assertThat(stream.read()).isEqualTo(-1);
    }

    assertThat(buf).isEqualTo(data);
  }

  // ---- error mapping ----

  @Test
  void notFoundMapsToNotFoundException() {
    String path = "/gone.parquet";
    mockServer.when(request(path).withMethod("GET")).respond(response().withStatusCode(404));

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
                stream.read();
              }
            })
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(path);
  }

  @Test
  void forbiddenMapsToExpiredUrlError() {
    String path = "/expired.parquet";
    mockServer.when(request(path).withMethod("GET")).respond(response().withStatusCode(403));

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
                stream.read();
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContainingAll("expired", "403");
  }

  @Test
  void unauthorizedMapsToExpiredUrlError() {
    String path = "/unauth.parquet";
    mockServer.when(request(path).withMethod("GET")).respond(response().withStatusCode(401));

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
                stream.read();
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContainingAll("expired", "401");
  }

  // ---- retries ----

  @Test
  void transientServerErrorIsRetried() throws IOException {
    byte[] data = new byte[] {42};
    String path = "/flaky.bin";

    // First two requests return 503; third returns the data.
    mockServer
        .when(request(path).withMethod("GET"), Times.exactly(2))
        .respond(response().withStatusCode(503));
    mockServer
        .when(request(path).withMethod("GET"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    byte[] buf = new byte[1];
    try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
      ((RangeReadable) stream).readFully(0, buf, 0, 1);
    }

    assertThat(buf[0]).isEqualTo((byte) 42);
  }

  @Test
  void exceededRetriesPropagate() {
    String path = "/always503.bin";
    // Always returns 503 — exhausts all retries.
    mockServer.when(request(path).withMethod("GET")).respond(response().withStatusCode(503));

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = fileIO.newInputFile(baseUrl + path).newStream()) {
                stream.read();
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContaining("503");
  }

  // ---- write/list/delete operations are unsupported ----

  @Test
  void writeOperationsThrowUnsupported() {
    assertThatThrownBy(() -> fileIO.newOutputFile(baseUrl + "/out"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support writes");
    assertThatThrownBy(() -> fileIO.deleteFile(baseUrl + "/del"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support deletes");
    assertThatThrownBy(() -> fileIO.deleteFiles(java.util.List.of(baseUrl + "/del")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support delete operations");
    assertThatThrownBy(() -> fileIO.listPrefix(baseUrl + "/prefix/"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support list operations");
    assertThatThrownBy(() -> fileIO.deletePrefix(baseUrl + "/prefix/"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support delete operations");
  }

  // ---- pre-signed URL map lookup ----

  @Test
  void resolvesPreSignedUrlFromMapByLogicalPath() throws IOException {
    byte[] data = new byte[] {9, 9, 9};
    String logicalPath = "s3://bucket/data/mapped.parquet";
    String signedPath = "/mapped-signed.bin";
    String signedUrl = baseUrl + signedPath + "?sig=abc";

    mockServer
        .when(request(signedPath).withMethod("GET").withHeader("Range", "bytes=0-2"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    HTTPFileIO mapped = new HTTPFileIO();
    mapped.initialize(ImmutableMap.of(HTTPFileIO.ENABLED, "true"));
    mapped.setPreSignedUrls(
        ImmutableMap.of(logicalPath, signedUrl), System.currentTimeMillis() + 3_600_000L);

    try {
      InputFile inputFile = mapped.newInputFile(logicalPath);
      assertThat(inputFile.location()).isEqualTo(logicalPath);

      byte[] buffer = new byte[3];
      try (SeekableInputStream stream = inputFile.newStream()) {
        ((RangeReadable) stream).readFully(0, buffer, 0, 3);
      }

      assertThat(buffer).isEqualTo(data);
    } finally {
      mapped.close();
    }
  }

  @Test
  void setPreSignedUrlsUpdatesPreSignedUrlsAndExpiration() {
    HTTPFileIO io = new HTTPFileIO();
    io.initialize(ImmutableMap.of(HTTPFileIO.ENABLED, "true"));
    Map<String, String> urls = ImmutableMap.of("s3://bucket/f.parquet", "https://signed/f");

    io.setPreSignedUrls(urls, 123L);

    assertThat(io.preSignedUrls()).isEqualTo(urls);
    assertThat(io.urlExpirationTimestampMs()).isEqualTo(123L);
    io.close();
  }

  // ---- executor-side refresh of an expired signed URL ----

  @Test
  void refreshesExpiredUrlAndRetriesRead() throws IOException {
    byte[] data = new byte[] {5, 6, 7};
    String path = "/refresh.bin";
    String staleUrl = baseUrl + path + "?sig=stale";
    String freshUrl = baseUrl + path + "?sig=fresh";

    mockServer
        .when(request(path).withMethod("GET").withQueryStringParameter("sig", "stale"))
        .respond(response().withStatusCode(403));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(staleUrl, freshUrl)));
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "fresh")
                .withHeader("Range", "bytes=0-2"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    refreshableFileIO = newRefreshableFileIO();

    byte[] buffer = new byte[3];
    try (SeekableInputStream stream = refreshableFileIO.newInputFile(staleUrl).newStream()) {
      ((RangeReadable) stream).readFully(0, buffer, 0, 3);
    }

    assertThat(buffer).isEqualTo(data);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(1));
  }

  @Test
  void refreshRequestCarriesPlanIdAndFilePath() throws IOException {
    byte[] data = new byte[] {5, 6, 7};
    String path = "/refresh-request.bin";
    String staleUrl = baseUrl + path + "?sig=stale";
    String freshUrl = baseUrl + path + "?sig=fresh";

    mockServer
        .when(request(path).withMethod("GET").withQueryStringParameter("sig", "stale"))
        .respond(response().withStatusCode(403));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(staleUrl, freshUrl)));
    mockServer
        .when(request(path).withMethod("GET").withQueryStringParameter("sig", "fresh"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    refreshableFileIO = newRefreshableFileIO();

    byte[] buffer = new byte[3];
    try (SeekableInputStream stream = refreshableFileIO.newInputFile(staleUrl).newStream()) {
      ((RangeReadable) stream).readFully(0, buffer, 0, 3);
    }

    HttpRequest[] recorded =
        mockServer.retrieveRecordedRequests(request("/presign").withMethod("POST"));
    assertThat(recorded).hasSize(1);
    // The refresh is plan-scoped and keyed on the InputFile's location, which is the value passed
    // to newInputFile (since no pre-signed URL map entry was set for it).
    assertThat(recorded[0].getBodyAsString())
        .contains("\"plan-id\" : \"test-plan-id\"")
        .contains("\"" + staleUrl + "\"");
  }

  @Test
  void refreshesExpiredUrlOnSequentialRead() throws IOException {
    byte[] data = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
    String path = "/refresh-seq.txt";
    String staleUrl = baseUrl + path + "?sig=stale";
    String freshUrl = baseUrl + path + "?sig=fresh";

    // The sequential read fetches a full chunk; the stale URL is rejected and only the refreshed
    // URL serves it.
    String chunkRange = String.format("bytes=0-%d", HTTPInputStream.CHUNK_SIZE - 1);
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "stale")
                .withHeader("Range", chunkRange))
        .respond(response().withStatusCode(403));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(staleUrl, freshUrl)));
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "fresh")
                .withHeader("Range", chunkRange))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    refreshableFileIO = newRefreshableFileIO();

    byte[] buf = new byte[data.length];
    int totalRead = 0;
    try (SeekableInputStream stream = refreshableFileIO.newInputFile(staleUrl).newStream()) {
      int bytesRead;
      while ((bytesRead = stream.read(buf, totalRead, buf.length - totalRead)) > 0) {
        totalRead += bytesRead;
      }
    }

    assertThat(totalRead).isEqualTo(data.length);
    assertThat(buf).isEqualTo(data);
  }

  @Test
  void refreshesExpiredUrlOnExists() {
    String path = "/refresh-exists.bin";
    String staleUrl = baseUrl + path + "?sig=stale";
    String freshUrl = baseUrl + path + "?sig=fresh";

    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "stale")
                .withHeader("Range", "bytes=0-0"))
        .respond(response().withStatusCode(403));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(staleUrl, freshUrl)));
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "fresh")
                .withHeader("Range", "bytes=0-0"))
        .respond(
            response()
                .withStatusCode(206)
                .withHeader("Content-Range", "bytes 0-0/100")
                .withBody(new byte[] {0}));

    refreshableFileIO = newRefreshableFileIO();

    assertThat(refreshableFileIO.newInputFile(staleUrl).exists()).isTrue();
  }

  @Test
  void existsReturnsFalseOnExpiredUrlWhenNoRefresherConfigured() {
    // Without a refresher, an expired URL cannot be re-signed, so exists() reports false rather
    // than throwing.
    String path = "/exists-expired.bin";
    mockServer
        .when(request(path).withMethod("GET").withHeader("Range", "bytes=0-0"))
        .respond(response().withStatusCode(403));

    assertThat(fileIO.newInputFile(baseUrl + path + "?sig=stale").exists()).isFalse();
  }

  @Test
  void refreshesExpiredUrlAndRetriesGetLength() {
    String path = "/refresh-length.bin";
    String staleUrl = baseUrl + path + "?sig=stale";
    String freshUrl = baseUrl + path + "?sig=fresh";

    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "stale")
                .withHeader("Range", "bytes=0-0"))
        .respond(response().withStatusCode(401));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(staleUrl, freshUrl)));
    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withQueryStringParameter("sig", "fresh")
                .withHeader("Range", "bytes=0-0"))
        .respond(
            response()
                .withStatusCode(206)
                .withHeader("Content-Range", "bytes 0-0/321")
                .withBody(new byte[] {0}));

    refreshableFileIO = newRefreshableFileIO();

    assertThat(refreshableFileIO.newInputFile(staleUrl).getLength()).isEqualTo(321L);
  }

  @Test
  void stillExpiredAfterOneRefreshRetryIsFatal() {
    String path = "/refresh-fails.bin";
    String staleUrl = baseUrl + path + "?sig=stale";
    String freshUrl = baseUrl + path + "?sig=fresh";

    // The re-signed URL is rejected too (e.g. the plan expired): every GET on this path returns
    // 403.
    mockServer.when(request(path).withMethod("GET")).respond(response().withStatusCode(403));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(staleUrl, freshUrl)));

    refreshableFileIO = newRefreshableFileIO();

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream =
                  refreshableFileIO.newInputFile(staleUrl).newStream()) {
                stream.read();
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContainingAll("expired", "403");

    // One refresh per fetch attempt, not an unbounded retry loop.
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(1));
  }

  @Test
  void noRefresherConfiguredMeansExpiredUrlIsStillFatal() {
    // The shared `fileIO` has no plan-id/presign-endpoint properties, so no refresher is
    // configured.
    assertThat(fileIO.refresher()).isNull();
  }

  @Test
  void refreshKeysOnLogicalPathWhenPreSignedUrlWasMapped() throws IOException {
    byte[] data = new byte[] {5, 6, 7};
    String logicalPath = "s3://bucket/mapped-refresh.parquet";
    String signedPath = "/mapped-refresh.bin";
    String staleUrl = baseUrl + signedPath + "?sig=stale";
    String freshUrl = baseUrl + signedPath + "?sig=fresh";

    mockServer
        .when(request(signedPath).withMethod("GET").withQueryStringParameter("sig", "stale"))
        .respond(response().withStatusCode(403));
    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(logicalPath, freshUrl)));
    mockServer
        .when(
            request(signedPath)
                .withMethod("GET")
                .withQueryStringParameter("sig", "fresh")
                .withHeader("Range", "bytes=0-2"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    refreshableFileIO = newRefreshableFileIO();
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPath, staleUrl), System.currentTimeMillis() + 3_600_000L);

    byte[] buffer = new byte[3];
    try (SeekableInputStream stream = refreshableFileIO.newInputFile(logicalPath).newStream()) {
      ((RangeReadable) stream).readFully(0, buffer, 0, 3);
    }

    assertThat(buffer).isEqualTo(data);
    HttpRequest[] recorded =
        mockServer.retrieveRecordedRequests(request("/presign").withMethod("POST"));
    assertThat(recorded).hasSize(1);
    // The refresh request is keyed on the file's logical path, not its (stale) pre-signed URL.
    assertThat(recorded[0].getBodyAsString())
        .contains("\"plan-id\" : \"test-plan-id\"")
        .contains("\"" + logicalPath + "\"");
  }

  // ---- proactive warm-up refresh ----

  @Test
  void warmUpRefreshesWhenCloseToExpiry() throws IOException {
    byte[] data = new byte[] {5, 6, 7};
    String logicalPath = "s3://bucket/warm-up.parquet";
    String signedPath = "/warm-up.bin";
    String staleUrl = baseUrl + signedPath + "?sig=stale";
    String freshUrl = baseUrl + signedPath + "?sig=fresh";

    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(logicalPath, freshUrl)));
    mockServer
        .when(
            request(signedPath)
                .withMethod("GET")
                .withQueryStringParameter("sig", "fresh")
                .withHeader("Range", "bytes=0-2"))
        .respond(
            response()
                .withStatusCode(206)
                .withContentType(MediaType.APPLICATION_OCTET_STREAM)
                .withBody(data));

    refreshableFileIO =
        newRefreshableFileIO(ImmutableMap.of(HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS, "60000"));
    // Held URL "expired" a second ago, so it is within any positive refresh threshold.
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPath, staleUrl), System.currentTimeMillis() - 1000L);

    refreshableFileIO.warmUp(List.of(logicalPath));

    assertThat(refreshableFileIO.preSignedUrls()).containsEntry(logicalPath, freshUrl);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(1));

    byte[] buffer = new byte[3];
    try (SeekableInputStream stream = refreshableFileIO.newInputFile(logicalPath).newStream()) {
      ((RangeReadable) stream).readFully(0, buffer, 0, 3);
    }

    assertThat(buffer).isEqualTo(data);
    // warmUp already replaced the stale URL, so the read needed no reactive refresh of its own.
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(1));
  }

  @Test
  void warmUpRefreshesUnionOfAllRegisteredLocations() {
    String logicalPathA = "s3://bucket/union-a.parquet";
    String logicalPathB = "s3://bucket/union-b.parquet";
    String staleUrlA = baseUrl + "/union-a.bin?sig=stale";
    String staleUrlB = baseUrl + "/union-b.bin?sig=stale";
    String freshUrlA = baseUrl + "/union-a.bin?sig=fresh";
    String freshUrlB = baseUrl + "/union-b.bin?sig=fresh";

    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(
                    PresignResponseParser.toJson(
                        ImmutablePresignResponse.builder()
                            .preSignedUrls(
                                ImmutableMap.of(
                                    logicalPathA, freshUrlA,
                                    logicalPathB, freshUrlB))
                            .urlExpirationTimestampMs(System.currentTimeMillis() + 3_600_000L)
                            .build())));

    refreshableFileIO =
        newRefreshableFileIO(ImmutableMap.of(HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS, "60000"));
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPathA, staleUrlA, logicalPathB, staleUrlB),
        System.currentTimeMillis() + 3_600_000L);

    // A is registered while the held URLs are not yet close to expiry, so no refresh happens yet.
    refreshableFileIO.warmUp(List.of(logicalPathA));
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(0));

    // The held URLs are now close to expiry; warming up B refreshes A and B together in one call,
    // proving the batch covers every location warmed up so far, not just the latest call's.
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPathA, staleUrlA, logicalPathB, staleUrlB),
        System.currentTimeMillis() - 1000L);
    refreshableFileIO.warmUp(List.of(logicalPathB));

    assertThat(refreshableFileIO.preSignedUrls())
        .containsEntry(logicalPathA, freshUrlA)
        .containsEntry(logicalPathB, freshUrlB);
    HttpRequest[] recorded =
        mockServer.retrieveRecordedRequests(request("/presign").withMethod("POST"));
    assertThat(recorded).hasSize(1);
    assertThat(recorded[0].getBodyAsString())
        .contains("\"" + logicalPathA + "\"")
        .contains("\"" + logicalPathB + "\"");
  }

  @Test
  void warmUpRefreshesAllTaskFilesInOneBatchCallNotOnePerFile() {
    // Simulates a task group of several files that all expired while queued, as BaseReader passes
    // every file's location to a single warmUp call. This must resolve in one batch /presign call,
    // not one call per file.
    int fileCount = 4;
    List<String> logicalPaths = Lists.newArrayList();
    Map<String, String> staleUrls = Maps.newHashMap();
    Map<String, String> freshUrls = Maps.newHashMap();
    for (int i = 0; i < fileCount; i++) {
      String logicalPath = String.format("s3://bucket/batch-%d.parquet", i);
      logicalPaths.add(logicalPath);
      staleUrls.put(logicalPath, baseUrl + String.format("/batch-%d.bin?sig=stale", i));
      freshUrls.put(logicalPath, baseUrl + String.format("/batch-%d.bin?sig=fresh", i));
    }

    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(
                    PresignResponseParser.toJson(
                        ImmutablePresignResponse.builder()
                            .preSignedUrls(freshUrls)
                            .urlExpirationTimestampMs(System.currentTimeMillis() + 3_600_000L)
                            .build())));

    refreshableFileIO =
        newRefreshableFileIO(ImmutableMap.of(HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS, "60000"));
    // All URLs "expired" together, e.g. a fast-expiring initial vend, so every file is due for
    // refresh by the time the task warms up.
    refreshableFileIO.setPreSignedUrls(staleUrls, System.currentTimeMillis() - 1000L);

    refreshableFileIO.warmUp(logicalPaths);

    assertThat(refreshableFileIO.preSignedUrls()).containsExactlyInAnyOrderEntriesOf(freshUrls);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(1));

    HttpRequest[] recorded =
        mockServer.retrieveRecordedRequests(request("/presign").withMethod("POST"));
    assertThat(recorded).hasSize(1);
    for (String logicalPath : logicalPaths) {
      assertThat(recorded[0].getBodyAsString()).contains("\"" + logicalPath + "\"");
    }
  }

  @Test
  void warmUpSingleFlightRefreshesOnce() throws Exception {
    String logicalPath = "s3://bucket/single-flight.parquet";
    String staleUrl = baseUrl + "/single-flight.bin?sig=stale";
    String freshUrl = baseUrl + "/single-flight.bin?sig=fresh";

    mockServer
        .when(request("/presign").withMethod("POST"))
        .respond(
            response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(presignResponseJson(logicalPath, freshUrl))
                .withDelay(TimeUnit.MILLISECONDS, 200));

    refreshableFileIO =
        newRefreshableFileIO(ImmutableMap.of(HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS, "60000"));
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPath, staleUrl), System.currentTimeMillis() - 1000L);

    int threadCount = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      CyclicBarrier barrier = new CyclicBarrier(threadCount);
      List<Future<?>> futures = Lists.newArrayList();
      for (int i = 0; i < threadCount; i++) {
        futures.add(
            executor.submit(
                () -> {
                  awaitBarrier(barrier);
                  refreshableFileIO.warmUp(List.of(logicalPath));
                }));
      }
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(refreshableFileIO.preSignedUrls()).containsEntry(logicalPath, freshUrl);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(1));
  }

  @Test
  void warmUpIsNoOpWhenNoRefresherConfigured() {
    String logicalPath = "s3://bucket/no-refresher.parquet";
    String currentUrl = baseUrl + "/no-refresher.bin?sig=current";

    HTTPFileIO io = new HTTPFileIO();
    io.initialize(ImmutableMap.of(HTTPFileIO.ENABLED, "true", CatalogProperties.URI, baseUrl));
    refreshableFileIO = io;
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPath, currentUrl), System.currentTimeMillis() - 1000L);

    refreshableFileIO.warmUp(List.of(logicalPath));

    assertThat(refreshableFileIO.preSignedUrls()).containsEntry(logicalPath, currentUrl);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(0));
  }

  @Test
  void warmUpIsNoOpWhenNoExpirationSet() {
    String logicalPath = "s3://bucket/no-expiration.parquet";
    String currentUrl = baseUrl + "/no-expiration.bin?sig=current";

    refreshableFileIO = newRefreshableFileIO();
    refreshableFileIO.setPreSignedUrls(ImmutableMap.of(logicalPath, currentUrl), 0L);

    refreshableFileIO.warmUp(List.of(logicalPath));

    assertThat(refreshableFileIO.preSignedUrls()).containsEntry(logicalPath, currentUrl);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(0));
  }

  @Test
  void warmUpIsNoOpWhenThresholdDisabled() {
    String logicalPath = "s3://bucket/threshold-disabled.parquet";
    String currentUrl = baseUrl + "/threshold-disabled.bin?sig=current";

    refreshableFileIO =
        newRefreshableFileIO(ImmutableMap.of(HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS, "0"));
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPath, currentUrl), System.currentTimeMillis() - 1000L);

    refreshableFileIO.warmUp(List.of(logicalPath));

    assertThat(refreshableFileIO.preSignedUrls()).containsEntry(logicalPath, currentUrl);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(0));
  }

  @Test
  void warmUpIsNoOpWhenNotYetCloseToExpiry() {
    String logicalPath = "s3://bucket/not-close.parquet";
    String currentUrl = baseUrl + "/not-close.bin?sig=current";

    refreshableFileIO =
        newRefreshableFileIO(ImmutableMap.of(HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS, "1000"));
    refreshableFileIO.setPreSignedUrls(
        ImmutableMap.of(logicalPath, currentUrl), System.currentTimeMillis() + 3_600_000L);

    refreshableFileIO.warmUp(List.of(logicalPath));

    assertThat(refreshableFileIO.preSignedUrls()).containsEntry(logicalPath, currentUrl);
    mockServer.verify(request("/presign").withMethod("POST"), VerificationTimes.exactly(0));
  }

  private static void awaitBarrier(CyclicBarrier barrier) {
    try {
      barrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }

  private HTTPFileIO newRefreshableFileIO() {
    return newRefreshableFileIO(ImmutableMap.of());
  }

  private HTTPFileIO newRefreshableFileIO(Map<String, String> extraProperties) {
    HTTPFileIO io = new HTTPFileIO();
    io.initialize(
        ImmutableMap.<String, String>builder()
            .put(HTTPFileIO.ENABLED, "true")
            .put(CatalogProperties.URI, baseUrl)
            .put(RESTCatalogProperties.REST_SCAN_PLAN_ID, "test-plan-id")
            .put(RESTCatalogProperties.REST_SCAN_PRESIGN_ENDPOINT, "/presign")
            .putAll(extraProperties)
            .buildKeepingLast());
    return io;
  }

  private static String presignResponseJson(String path, String freshUrl) {
    return PresignResponseParser.toJson(
        ImmutablePresignResponse.builder()
            .preSignedUrls(ImmutableMap.of(path, freshUrl))
            .urlExpirationTimestampMs(System.currentTimeMillis() + 3_600_000L)
            .build());
  }
}
