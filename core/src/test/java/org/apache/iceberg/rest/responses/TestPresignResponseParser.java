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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestPresignResponseParser {

  @Test
  public void nullResponse() {
    assertThatThrownBy(() -> PresignResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse presign response from null object");

    assertThatThrownBy(() -> PresignResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid presign response: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> PresignResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: pre-signed-urls");

    assertThatThrownBy(
            () ->
                PresignResponseParser.fromJson(
                    "{\"pre-signed-urls\" : {\"s3://bucket/file.parquet\" : \"https://x\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: url-expiration-timestamp-ms");
  }

  @Test
  public void invalidPreSignedUrls() {
    PresignResponse response =
        ImmutablePresignResponse.builder()
            .preSignedUrls(ImmutableMap.of())
            .urlExpirationTimestampMs(1781486400000L)
            .build();

    assertThatThrownBy(response::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid pre-signed URLs: null or empty");
  }

  @Test
  public void roundTripSerde() {
    PresignResponse response =
        ImmutablePresignResponse.builder()
            .preSignedUrls(
                ImmutableMap.of(
                    "s3://bucket/data/file1.parquet", "https://signed.example.com/file1?sig=abc",
                    "s3://bucket/data/file2.parquet", "https://signed.example.com/file2?sig=def"))
            .urlExpirationTimestampMs(1781486400000L)
            .build();

    String json = PresignResponseParser.toJson(response, true);
    assertThat(PresignResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"pre-signed-urls\" : {\n"
                + "    \"s3://bucket/data/file1.parquet\" : \"https://signed.example.com/file1?sig=abc\",\n"
                + "    \"s3://bucket/data/file2.parquet\" : \"https://signed.example.com/file2?sig=def\"\n"
                + "  },\n"
                + "  \"url-expiration-timestamp-ms\" : 1781486400000\n"
                + "}");
  }
}
