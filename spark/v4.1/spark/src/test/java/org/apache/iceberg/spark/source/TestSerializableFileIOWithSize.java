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
package org.apache.iceberg.spark.source;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkSigning;
import org.junit.jupiter.api.Test;

class TestSerializableFileIOWithSize {

  @Test
  void newInputFileWithLength() {
    FileIO mockFileIO = mock(FileIO.class);
    FileIO serializableFileIO = SerializableFileIOWithSize.wrap(mockFileIO);
    String path = "gs://bucket/path/to/file.parquet";
    long length = 1024L;

    serializableFileIO.newInputFile(path, length);

    verify(mockFileIO).newInputFile(path, length);
  }

  @Test
  void newInputFileWithoutLength() {
    FileIO mockFileIO = mock(FileIO.class);
    FileIO serializableFileIO = SerializableFileIOWithSize.wrap(mockFileIO);
    String path = "gs://bucket/path/to/file.parquet";

    serializableFileIO.newInputFile(path);

    verify(mockFileIO).newInputFile(path);
  }

  @Test
  void bulkSignDelegatesWhenSupported() {
    FileIO delegate = mock(FileIO.class, withSettings().extraInterfaces(SupportsBulkSigning.class));
    FileIO serializableFileIO = SerializableFileIOWithSize.wrap(delegate);
    List<String> locations = List.of("s3://bucket/data.parquet", "s3://bucket/deletes.parquet");

    ((SupportsBulkSigning) serializableFileIO).bulkSign(locations);

    verify((SupportsBulkSigning) delegate).bulkSign(locations);
  }

  @Test
  void bulkSignIsNoOpWhenDelegateDoesNotSupportIt() {
    FileIO delegate = mock(FileIO.class);
    FileIO serializableFileIO = SerializableFileIOWithSize.wrap(delegate);

    ((SupportsBulkSigning) serializableFileIO).bulkSign(List.of("s3://bucket/data.parquet"));

    verifyNoInteractions(delegate);
  }
}
