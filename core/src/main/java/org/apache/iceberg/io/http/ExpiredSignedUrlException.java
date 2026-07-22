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
import java.util.Locale;
import org.apache.hc.core5.http.HttpStatus;

/**
 * Internal signal thrown when a storage response (403/401) indicates that a pre-signed URL has
 * expired or is otherwise invalid. Callers catch this to decide whether to refresh the URL and
 * retry, or to surface a final, descriptive error.
 */
final class ExpiredSignedUrlException extends IOException {
  private final int statusCode;

  ExpiredSignedUrlException(int statusCode) {
    this.statusCode = statusCode;
  }

  int statusCode() {
    return statusCode;
  }

  /**
   * Whether a storage status code indicates an expired or otherwise unauthorized pre-signed URL.
   */
  static boolean indicatesExpiry(int statusCode) {
    return statusCode == HttpStatus.SC_FORBIDDEN || statusCode == HttpStatus.SC_UNAUTHORIZED;
  }

  /** Builds the final, descriptive error surfaced when a URL is expired and cannot be refreshed. */
  IOException toIOException(String url) {
    return new IOException(
        String.format(
            Locale.ROOT, "Presigned URL is expired or invalid (HTTP %d): %s", statusCode, url));
  }
}
