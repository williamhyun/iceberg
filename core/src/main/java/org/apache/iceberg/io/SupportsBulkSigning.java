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

/**
 * Extension for {@link FileIO} implementations that can obtain signatures for several object
 * locations in a single call.
 *
 * <p>This lets a reader that is about to read a known set of files collapse into a single
 * remote-signing request what would otherwise be one request per object, reducing catalog
 * round-trips. Implementations should sign the given locations and make the results available to
 * the subsequent reads. Locations that are already signed may be skipped.
 */
public interface SupportsBulkSigning {

  /**
   * Signs the given object locations in a single request, in preparation for imminent reads.
   *
   * <p>This is a best-effort optimization: if an implementation cannot sign in bulk (or the feature
   * is disabled), it should be a no-op and let the per-request signing path handle the reads. POC
   * limitation: bulk signing operation may propagate errors and fail the scan.
   *
   * @param locations the object locations that are about to be read
   */
  void bulkSign(Iterable<String> locations);
}
