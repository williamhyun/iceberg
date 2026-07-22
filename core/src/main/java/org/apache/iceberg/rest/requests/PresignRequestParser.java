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
import org.apache.iceberg.util.JsonUtil;

public class PresignRequestParser {

  private static final String PLAN_ID = "plan-id";
  private static final String FILE_PATHS = "file-paths";

  private PresignRequestParser() {}

  public static String toJson(PresignRequest request) {
    return toJson(request, false);
  }

  public static String toJson(PresignRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(PresignRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid presign request: null");

    gen.writeStartObject();
    gen.writeStringField(PLAN_ID, request.planId());
    JsonUtil.writeStringArray(FILE_PATHS, request.filePaths(), gen);
    gen.writeEndObject();
  }

  public static PresignRequest fromJson(String json) {
    return JsonUtil.parse(json, PresignRequestParser::fromJson);
  }

  public static PresignRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse presign request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse presign request from non-object: %s", json);

    String planId = JsonUtil.getString(PLAN_ID, json);
    List<String> filePaths = JsonUtil.getStringList(FILE_PATHS, json);

    return ImmutablePresignRequest.builder().planId(planId).filePaths(filePaths).build();
  }
}
