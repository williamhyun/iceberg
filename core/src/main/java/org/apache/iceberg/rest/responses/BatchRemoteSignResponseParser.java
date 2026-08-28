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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class BatchRemoteSignResponseParser {

  private static final String RESPONSES = "responses";

  private BatchRemoteSignResponseParser() {}

  public static String toJson(BatchRemoteSignResponse response) {
    return toJson(response, false);
  }

  public static String toJson(BatchRemoteSignResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(BatchRemoteSignResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid batch remote sign response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(RESPONSES);
    for (RemoteSignResponse signResponse : response.responses()) {
      RemoteSignResponseParser.toJson(signResponse, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchRemoteSignResponse fromJson(String json) {
    return JsonUtil.parse(json, BatchRemoteSignResponseParser::fromJson);
  }

  public static BatchRemoteSignResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch remote sign response from null object");

    JsonNode responsesNode = JsonUtil.get(RESPONSES, json);
    Preconditions.checkArgument(
        responsesNode.isArray(),
        "Cannot parse batch remote sign response from non-array: %s",
        responsesNode);

    List<RemoteSignResponse> responses = Lists.newArrayList();
    for (JsonNode node : responsesNode) {
      responses.add(RemoteSignResponseParser.fromJson(node));
    }

    return ImmutableBatchRemoteSignResponse.builder().responses(responses).build();
  }
}
