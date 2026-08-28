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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Collections;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestBatchRemoteSignRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> BatchRemoteSignRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch remote sign request from null object");

    assertThatThrownBy(() -> BatchRemoteSignRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch remote sign request: null");
  }

  @Test
  public void missingRequests() {
    assertThatThrownBy(() -> BatchRemoteSignRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: requests");
  }

  @Test
  public void requestsNotAnArray() {
    assertThatThrownBy(() -> BatchRemoteSignRequestParser.fromJson("{\"requests\": {}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse batch remote sign request from non-array");
  }

  @Test
  public void roundTripSerde() {
    RemoteSignRequest first =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("http://localhost:49208/bucket/data.parquet"))
            .method("GET")
            .region("us-west-2")
            .headers(ImmutableMap.of("Host", Collections.singletonList("bucket.s3.amazonaws.com")))
            .properties(ImmutableMap.of("rest-scan-plan-id", "plan-123"))
            .build();
    RemoteSignRequest second =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("http://localhost:49208/bucket/deletes.parquet"))
            .method("GET")
            .region("us-west-2")
            .headers(ImmutableMap.of("Host", Collections.singletonList("bucket.s3.amazonaws.com")))
            .properties(ImmutableMap.of("rest-scan-plan-id", "plan-123"))
            .build();

    BatchRemoteSignRequest request =
        ImmutableBatchRemoteSignRequest.builder().requests(ImmutableList.of(first, second)).build();

    String json = BatchRemoteSignRequestParser.toJson(request, true);
    assertThat(BatchRemoteSignRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json).contains("\"requests\"");
  }

  @Test
  public void roundTripEmptyBatch() {
    BatchRemoteSignRequest request =
        ImmutableBatchRemoteSignRequest.builder().requests(ImmutableList.of()).build();

    String json = BatchRemoteSignRequestParser.toJson(request);
    assertThat(BatchRemoteSignRequestParser.fromJson(json)).isEqualTo(request);
  }
}
