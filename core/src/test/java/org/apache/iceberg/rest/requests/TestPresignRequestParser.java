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
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestPresignRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> PresignRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse presign request from null object");

    assertThatThrownBy(() -> PresignRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid presign request: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> PresignRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: plan-id");

    assertThatThrownBy(() -> PresignRequestParser.fromJson("{\"plan-id\" : \"plan-123\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing list: file-paths");
  }

  @Test
  public void invalidFilePaths() {
    PresignRequest request =
        ImmutablePresignRequest.builder().planId("plan-123").filePaths(List.of()).build();

    assertThatThrownBy(request::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file paths: null or empty");
  }

  @Test
  public void roundTripSerde() {
    PresignRequest request =
        ImmutablePresignRequest.builder()
            .planId("plan-123")
            .filePaths(List.of("s3://bucket/data/file1.parquet", "s3://bucket/data/file2.parquet"))
            .build();

    String json = PresignRequestParser.toJson(request, true);
    assertThat(PresignRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"plan-id\" : \"plan-123\",\n"
                + "  \"file-paths\" : [ \"s3://bucket/data/file1.parquet\", \"s3://bucket/data/file2.parquet\" ]\n"
                + "}");
  }
}
