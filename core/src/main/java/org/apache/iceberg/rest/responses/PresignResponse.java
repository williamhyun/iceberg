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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;
import org.immutables.value.Value;

/** The response containing refreshed pre-signed URLs for a set of previously planned files. */
@Value.Immutable
public interface PresignResponse extends RESTResponse {
  Map<String, String> preSignedUrls();

  long urlExpirationTimestampMs();

  @Override
  default void validate() {
    Preconditions.checkArgument(
        preSignedUrls() != null && !preSignedUrls().isEmpty(),
        "Invalid pre-signed URLs: null or empty");
  }
}
