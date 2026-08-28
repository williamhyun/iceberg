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
import java.net.URI;
import java.util.Collections;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestBatchRemoteSignResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> BatchRemoteSignResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch remote sign response from null object");

    assertThatThrownBy(() -> BatchRemoteSignResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch remote sign response: null");
  }

  @Test
  public void missingResponses() {
    assertThatThrownBy(() -> BatchRemoteSignResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: responses");
  }

  @Test
  public void responsesNotAnArray() {
    assertThatThrownBy(() -> BatchRemoteSignResponseParser.fromJson("{\"responses\": {}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse batch remote sign response from non-array");
  }

  @Test
  public void roundTripSerde() {
    RemoteSignResponse first =
        ImmutableRemoteSignResponse.builder()
            .uri(URI.create("http://localhost:49208/bucket/data.parquet"))
            .headers(
                ImmutableMap.of("Authorization", Collections.singletonList("AWS4-HMAC-SHA256 ...")))
            .build();
    RemoteSignResponse second =
        ImmutableRemoteSignResponse.builder()
            .uri(URI.create("http://localhost:49208/bucket/deletes.parquet"))
            .headers(
                ImmutableMap.of("Authorization", Collections.singletonList("AWS4-HMAC-SHA256 ...")))
            .build();

    BatchRemoteSignResponse response =
        ImmutableBatchRemoteSignResponse.builder()
            .responses(ImmutableList.of(first, second))
            .build();

    String json = BatchRemoteSignResponseParser.toJson(response, true);
    assertThat(BatchRemoteSignResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json).contains("\"responses\"");
  }

  @Test
  public void roundTripEmptyBatch() {
    BatchRemoteSignResponse response =
        ImmutableBatchRemoteSignResponse.builder().responses(ImmutableList.of()).build();

    String json = BatchRemoteSignResponseParser.toJson(response);
    assertThat(BatchRemoteSignResponseParser.fromJson(json)).isEqualTo(response);
  }
}
