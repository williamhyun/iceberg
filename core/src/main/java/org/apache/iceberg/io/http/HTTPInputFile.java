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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTSignedUrlRefresher;

/**
 * An {@link InputFile} whose immutable, logical {@link #location()} is resolved to a target HTTP
 * URL to read from, typically a pre-signed object-store URL that encodes all required auth in its
 * query parameters.
 *
 * <p>When the content length is known at construction time it is returned directly from {@link
 * #getLength()} without a network round-trip. Otherwise it is fetched on the first call via a
 * {@code GET Range: bytes=0-0} request, which (unlike HEAD) is compatible with pre-signed GET URLs.
 *
 * <p>Reads target the URL this file was constructed with. If a storage response indicates expiry
 * (HTTP 403/401) and a {@link RESTSignedUrlRefresher} is configured, {@link #location()} is
 * re-signed and the read retried once; otherwise the failure is surfaced.
 */
class HTTPInputFile implements InputFile {
  private static final long UNKNOWN_LENGTH = -1L;

  private final CloseableHttpClient client;
  @Nullable private final RESTSignedUrlRefresher refresher;
  private final String location;
  private final String url;

  private long length;

  HTTPInputFile(
      CloseableHttpClient client,
      @Nullable RESTSignedUrlRefresher refresher,
      String location,
      String url) {
    this(client, refresher, location, url, UNKNOWN_LENGTH);
  }

  HTTPInputFile(
      CloseableHttpClient client,
      @Nullable RESTSignedUrlRefresher refresher,
      String location,
      String url,
      long length) {
    Preconditions.checkNotNull(client, "Invalid HTTP client: null");
    Preconditions.checkNotNull(location, "Invalid location: null");
    Preconditions.checkNotNull(url, "Invalid url: null");
    this.client = client;
    this.refresher = refresher;
    this.location = location;
    this.url = url;
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == UNKNOWN_LENGTH) {
      this.length = fetchContentLength();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new HTTPInputStream(client, refresher, location, url);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    return existsAt(url, true);
  }

  /**
   * Uses a zero-range GET so the request is compatible with pre-signed GET URLs, unlike HEAD. On an
   * expired URL, refreshes and retries once before falling back to {@code false}, matching the
   * pre-refresh behavior of reporting non-existence rather than throwing.
   */
  private boolean existsAt(String currentUrl, boolean allowRefresh) {
    try {
      HttpGet request = new HttpGet(currentUrl);
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");
      int statusCode = client.execute(request, ClassicHttpResponse::getCode);

      if (statusCode == HttpStatus.SC_PARTIAL_CONTENT || statusCode == HttpStatus.SC_OK) {
        return true;
      }

      if (allowRefresh
          && ExpiredSignedUrlException.indicatesExpiry(statusCode)
          && refresher != null) {
        return existsAt(refresher.refresh(location), false);
      }

      return false;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to check existence of %s", location);
    }
  }

  private long fetchContentLength() {
    return fetchContentLengthAt(url, true);
  }

  /**
   * Fetches the content length by issuing {@code GET Range: bytes=0-0} and parsing the {@code
   * Content-Range} response header. This approach works with pre-signed GET URLs, unlike a {@code
   * HEAD} request whose signature would be rejected by S3/MinIO.
   */
  private long fetchContentLengthAt(String currentUrl, boolean allowRefresh) {
    try {
      HttpGet request = new HttpGet(currentUrl);
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");

      return client.execute(
          request,
          response -> {
            int statusCode = response.getCode();

            if (statusCode == HttpStatus.SC_NOT_FOUND) {
              throw new NotFoundException("Location does not exist: %s", location);
            } else if (ExpiredSignedUrlException.indicatesExpiry(statusCode)) {
              throw new ExpiredSignedUrlException(statusCode);
            }

            // 206 Partial Content: parse total from "Content-Range: bytes 0-0/TOTAL"
            if (statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
              Header contentRange = response.getFirstHeader("Content-Range");
              if (contentRange != null) {
                long total = parseTotalFromContentRange(contentRange.getValue());
                if (total >= 0) {
                  return total;
                }
              }
            }

            // 200 OK: server returned full content, use Content-Length
            if (statusCode == HttpStatus.SC_OK) {
              return parseLengthFrom200(response);
            }

            return UNKNOWN_LENGTH;
          });
    } catch (ExpiredSignedUrlException e) {
      if (allowRefresh && refresher != null) {
        return fetchContentLengthAt(refresher.refresh(location), false);
      }

      throw new RuntimeIOException(
          e.toIOException(currentUrl), "Failed to fetch content length for %s", location);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to fetch content length for %s", location);
    }
  }

  /** Extracts content length from a 200 response via entity or {@code Content-Length} header. */
  private static long parseLengthFrom200(ClassicHttpResponse response) {
    long contentLength =
        response.getEntity() != null ? response.getEntity().getContentLength() : UNKNOWN_LENGTH;
    if (contentLength >= 0) {
      return contentLength;
    }

    Header header = response.getFirstHeader("Content-Length");
    if (header != null) {
      try {
        return Long.parseLong(header.getValue());
      } catch (NumberFormatException e) {
        // fall through to UNKNOWN_LENGTH
      }
    }

    return UNKNOWN_LENGTH;
  }

  /**
   * Parses the total object size from a {@code Content-Range} header value such as {@code bytes
   * 0-0/12345}.
   */
  private static long parseTotalFromContentRange(String contentRange) {
    int slash = contentRange.lastIndexOf('/');
    if (slash < 0) {
      return UNKNOWN_LENGTH;
    }

    String totalStr = contentRange.substring(slash + 1).trim();
    if ("*".equals(totalStr)) {
      return UNKNOWN_LENGTH;
    }

    try {
      return Long.parseLong(totalStr);
    } catch (NumberFormatException e) {
      return UNKNOWN_LENGTH;
    }
  }
}
