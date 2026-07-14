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
package org.apache.iceberg.io;

import java.util.Collection;
import java.util.Map;

/**
 * This interface is intended as an extension for {@link FileIO} implementations to be able to
 * receive a mapping of immutable file-path to ephemeral, pre-signed HTTP URL for the files of a
 * single scan.
 *
 * <p>The map is passed directly to the FileIO instance rather than through {@link
 * FileIO#initialize(Map)}, since pre-signed URLs are as sensitive as credentials and should not be
 * exposed through the generic, frequently-logged {@link FileIO#properties()} map.
 */
public interface SupportsPreSignedUrls {

  /**
   * Sets the pre-signed URLs available for this FileIO's files, replacing any previously set.
   *
   * @param preSignedUrls a map of immutable file-path to a pre-signed HTTP URL that can be used to
   *     read that file's contents
   * @param urlExpirationTimestampMs the timestamp, as milliseconds since the Unix epoch, when the
   *     URLs in {@code preSignedUrls} expire
   */
  void setPreSignedUrls(Map<String, String> preSignedUrls, long urlExpirationTimestampMs);

  /** The pre-signed URLs most recently set via {@link #setPreSignedUrls(Map, long)}. */
  Map<String, String> preSignedUrls();

  /**
   * Registers {@code locations} as files this instance is about to read and, if the URLs it
   * currently holds for them are close enough to expiry, proactively refreshes them in a single
   * batch. A no-op when refresh is not configured or the held URLs are not yet close to expiry.
   *
   * @param locations immutable file-paths this instance expects to read soon
   */
  void warmUp(Collection<String> locations);
}
