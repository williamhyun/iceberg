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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class BatchRemoteSignRequestParser {

  private static final String REQUESTS = "requests";

  private BatchRemoteSignRequestParser() {}

  public static String toJson(BatchRemoteSignRequest request) {
    return toJson(request, false);
  }

  public static String toJson(BatchRemoteSignRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(BatchRemoteSignRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid batch remote sign request: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(REQUESTS);
    for (RemoteSignRequest signRequest : request.requests()) {
      RemoteSignRequestParser.toJson(signRequest, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchRemoteSignRequest fromJson(String json) {
    return JsonUtil.parse(json, BatchRemoteSignRequestParser::fromJson);
  }

  public static BatchRemoteSignRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch remote sign request from null object");

    JsonNode requestsNode = JsonUtil.get(REQUESTS, json);
    Preconditions.checkArgument(
        requestsNode.isArray(),
        "Cannot parse batch remote sign request from non-array: %s",
        requestsNode);

    List<RemoteSignRequest> requests = Lists.newArrayList();
    for (JsonNode node : requestsNode) {
      requests.add(RemoteSignRequestParser.fromJson(node));
    }

    return ImmutableBatchRemoteSignRequest.builder().requests(requests).build();
  }
}
