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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class PresignResponseParser {

  private static final String PRE_SIGNED_URLS = "pre-signed-urls";
  private static final String URL_EXPIRATION_TIMESTAMP_MS = "url-expiration-timestamp-ms";

  private PresignResponseParser() {}

  public static String toJson(PresignResponse response) {
    return toJson(response, false);
  }

  public static String toJson(PresignResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(PresignResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid presign response: null");

    gen.writeStartObject();
    JsonUtil.writeStringMap(PRE_SIGNED_URLS, response.preSignedUrls(), gen);
    gen.writeNumberField(URL_EXPIRATION_TIMESTAMP_MS, response.urlExpirationTimestampMs());
    gen.writeEndObject();
  }

  public static PresignResponse fromJson(String json) {
    return JsonUtil.parse(json, PresignResponseParser::fromJson);
  }

  public static PresignResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse presign response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse presign response from non-object: %s", json);

    Map<String, String> preSignedUrls = JsonUtil.getStringMap(PRE_SIGNED_URLS, json);
    long urlExpirationTimestampMs = JsonUtil.getLong(URL_EXPIRATION_TIMESTAMP_MS, json);

    return ImmutablePresignResponse.builder()
        .preSignedUrls(preSignedUrls)
        .urlExpirationTimestampMs(urlExpirationTimestampMs)
        .build();
  }
}
