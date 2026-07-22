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

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.http.HTTPFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.PresignedUrlVendingAdapter;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.requests.PresignRequestParser;
import org.apache.iceberg.rest.responses.PresignResponse;
import org.apache.iceberg.rest.responses.PresignResponseParser;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves {@link HTTPFileIO} lets a Spark client read a table entirely through mock "presigned"
 * (plain HTTP, query-signed) URLs vended by a REST catalog's server-side scan-planning endpoint,
 * across position deletes, equality deletes, and deletion vectors.
 *
 * <p>Data and delete files are written for real, then relocated via {@link Table#newRewrite()} to
 * HTTP URLs served by a small Jetty {@link ResourceHandler}. Each data file is relocated before its
 * delete file is written, so any back-reference (a position delete's row content or a DV's {@code
 * referencedDataFile}) already points at the final HTTP location. Originals are deleted right after
 * relocation, so a successful read can only have gone through {@link HTTPFileIO}.
 *
 * <p>{@link #readLargeDataFileAcrossConcurrentScanTaskGroupsViaPresignedUrls()} additionally covers
 * one large file, planned as a single {@code FileScanTask}, that Spark splits client-side into
 * several {@code ScanTaskGroup}s reading the same URL concurrently.
 */
public class TestReadViaPresignedUrls extends TestBaseWithCatalog {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  // Small enough that a sub-MB data file spans several row groups, so it splits into multiple
  // ScanTaskGroups.
  private static final long LARGE_FILE_ROW_GROUP_SIZE_BYTES = 32 * 1024L;
  private static final int LARGE_FILE_ROW_GROUP_CHECK_MIN_RECORD_COUNT = 10;
  private static final int LARGE_FILE_ROW_COUNT = 600;
  private static final int LARGE_FILE_ROW_VALUE_BYTES = 800;

  @TempDir private static Path servableFilesDir;
  private static Server httpServer;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.<String, String>builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .put(
                RESTCatalogProperties.SCAN_PLANNING_MODE,
                RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
            .put(HTTPFileIO.ENABLED, "true")
            .build()
      }
    };
  }

  @BeforeAll
  public static void startHttpServer() throws Exception {
    httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setBaseResource(
        ResourceFactory.of(resourceHandler).newResource(servableFilesDir));
    resourceHandler.setDirAllowed(false);
    resourceHandler.setAcceptRanges(true);
    httpServer.setHandler(resourceHandler);
    httpServer.start();
  }

  @AfterAll
  public static void stopHttpServer() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  @TestTemplate
  public void readWithPositionDeletesViaPresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "position_deletes_table");
    Table table =
        restCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, newLocalOutputFile("data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    // Relocate the data file first: a v2 position delete embeds the data file location in its row
    // content, so it must be written against the data file's final (HTTP) location.
    DataFile relocatedDataFile = relocateDataFile(table, dataFile);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(Pair.of(relocatedDataFile.location(), 0L)); // delete id = 1
    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, newLocalOutputFile("pos-deletes"), null, deletes, 2);
    table.newRowDelta().addDeletes(posDeletes.first()).commit();

    relocateDeleteFile(table, posDeletes.first());
    assertServerSidePlanningEngaged(ident);

    List<Object[]> results = sql("SELECT id, data FROM %s ORDER BY id", tableName(ident.name()));
    assertThat(results).containsExactly(row(2, "b"), row(3, "c"));
  }

  @TestTemplate
  public void readWithEqualityDeletesViaPresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "equality_deletes_table");
    Table table =
        restCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, newLocalOutputFile("data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    relocateDataFile(table, dataFile);

    Schema deleteRowSchema = SCHEMA.select("id");
    Record deleteRecord = GenericRecord.create(deleteRowSchema);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            newLocalOutputFile("eq-deletes"),
            Lists.newArrayList(deleteRecord.copy("id", 2)), // delete id = 2
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    relocateDeleteFile(table, eqDeletes);
    assertServerSidePlanningEngaged(ident);

    List<Object[]> results = sql("SELECT id, data FROM %s ORDER BY id", tableName(ident.name()));
    assertThat(results).containsExactly(row(1, "a"), row(3, "c"));
  }

  @TestTemplate
  public void readWithDeletionVectorsViaPresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "dv_table");
    Table table =
        restCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, newLocalOutputFile("data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    // Relocate the data file first so the DV's referencedDataFile (set below) already points at its
    // final (HTTP) location.
    DataFile relocatedDataFile = relocateDataFile(table, dataFile);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(Pair.of(relocatedDataFile.location(), 0L)); // delete id = 1
    // formatVersion 3 makes FileHelpers write a real Puffin DV; the OutputFile arg is unused.
    Pair<DeleteFile, CharSequenceSet> dv =
        FileHelpers.writeDeleteFile(table, null, null, deletes, 3);
    table.newRowDelta().addDeletes(dv.first()).commit();

    relocateDeleteFile(table, dv.first());
    assertServerSidePlanningEngaged(ident);

    List<Object[]> results = sql("SELECT id, data FROM %s ORDER BY id", tableName(ident.name()));
    assertThat(results).containsExactly(row(2, "b"), row(3, "c"));
  }

  /**
   * One large data file, planned as a single {@code FileScanTask}, is split client-side into
   * multiple {@code ScanTaskGroup}s that concurrently range-read the same presigned URL.
   */
  @TestTemplate
  public void readLargeDataFileAcrossConcurrentScanTaskGroupsViaPresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "large_file_table");
    Table table =
        restCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    // Force many small row groups and a matching small split target so the one data file below is
    // divided into several concurrently-readable ScanTaskGroups.
    table
        .updateProperties()
        .set(
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            String.valueOf(LARGE_FILE_ROW_GROUP_SIZE_BYTES))
        .set(
            TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
            String.valueOf(LARGE_FILE_ROW_GROUP_CHECK_MIN_RECORD_COUNT))
        .set(TableProperties.SPLIT_SIZE, String.valueOf(LARGE_FILE_ROW_GROUP_SIZE_BYTES))
        .commit();

    Map<Integer, String> expectedValuesById =
        wideRows(LARGE_FILE_ROW_COUNT, LARGE_FILE_ROW_VALUE_BYTES);
    List<Record> records =
        expectedValuesById.entrySet().stream()
            .map(
                entry ->
                    GenericRecord.create(SCHEMA)
                        .copy("id", entry.getKey(), "data", entry.getValue()))
            .collect(Collectors.toList());
    DataFile dataFile = FileHelpers.writeDataFile(table, newLocalOutputFile("large-data"), records);
    table.newAppend().appendFile(dataFile).commit();

    relocateDataFile(table, dataFile);
    assertServerSidePlanningEngaged(ident);

    Dataset<Row> df = spark.sql(String.format("SELECT id, data FROM %s", tableName(ident.name())));
    assertThat(df.rdd().getNumPartitions())
        .as(
            "A data file this large, with a small split target size, should be planned into "
                + "more than one Spark input partition")
        .isGreaterThan(1);

    List<Row> rows = df.collectAsList();
    assertThat(rows).hasSize(LARGE_FILE_ROW_COUNT);
    assertThat(rows.stream().map(row -> row.getInt(0)).collect(Collectors.toSet()))
        .isEqualTo(expectedValuesById.keySet());
    for (Row row : rows) {
      assertThat(row.getString(1)).isEqualTo(expectedValuesById.get(row.getInt(0)));
    }
  }

  /**
   * A short-lived vended URL, combined with a generous refresh threshold, puts {@code
   * HTTPFileIO#warmUp} inside its refresh window as soon as {@code BaseReader} calls it -- before
   * any read is attempted. Unlike the other tests here, the table keeps its original (temp-dir)
   * locations; only the {@code pre-signed-urls} map, populated by {@link
   * PresignedUrlVendingAdapter}, carries the HTTP URLs, so a successful read also proves the
   * proactive refresh replaced the map entry before {@code HTTPFileIO} ever had to fall back to its
   * reactive 403 path.
   *
   * <p>The table has three small data files, all of which are bin-packed into a single {@code
   * ScanTaskGroup} (and therefore a single Spark partition), so a successful read with exactly one
   * {@code /presign} hit also proves {@code warmUp} re-signed every file this task needed in one
   * batch call rather than once per file.
   */
  @TestTemplate
  public void warmUpProactivelyRefreshesBeforeReadViaPresignedUrls() throws Exception {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "proactive_warm_up_table");
    AtomicInteger presignHits = new AtomicInteger();

    HadoopCatalog backendCatalog =
        new HadoopCatalog(new Configuration(), "file:" + temp.resolve("proactive-backend"));
    // A 1ms lifetime guarantees every vended URL is already inside any positive refresh threshold
    // by the time warmUp runs.
    PresignedUrlVendingAdapter adapter =
        new PresignedUrlVendingAdapter(backendCatalog, this::presignServedLocation, 1L);
    RESTCatalogServlet servlet = new PresignCountingServlet(adapter, presignHits);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(servlet), "/*");

    Server presignCatalogServer =
        new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    presignCatalogServer.setHandler(context);
    presignCatalogServer.start();
    try {
      String catalogUri = presignCatalogServer.getURI().toString();
      String catalogName = "proactive_presign_catalog";
      spark.conf().set("spark.sql.catalog." + catalogName, SparkCatalog.class.getName());
      spark.conf().set("spark.sql.catalog." + catalogName + ".type", "rest");
      spark.conf().set("spark.sql.catalog." + catalogName + ".cache-enabled", "false");
      spark.conf().set("spark.sql.catalog." + catalogName + ".uri", catalogUri);
      spark
          .conf()
          .set(
              "spark.sql.catalog." + catalogName + "." + RESTCatalogProperties.SCAN_PLANNING_MODE,
              RESTCatalogProperties.ScanPlanningMode.SERVER.modeName());
      spark.conf().set("spark.sql.catalog." + catalogName + "." + HTTPFileIO.ENABLED, "true");
      spark
          .conf()
          .set(
              "spark.sql.catalog." + catalogName + "." + HTTPFileIO.PRESIGN_REFRESH_THRESHOLD_MS,
              String.valueOf(TimeUnit.MINUTES.toMillis(10)));

      RESTCatalog writeCatalog = new RESTCatalog();
      writeCatalog.setConf(new Configuration());
      writeCatalog.initialize(
          "proactive-warm-up-write", ImmutableMap.of(CatalogProperties.URI, catalogUri));
      try {
        Table table =
            writeCatalog.createTable(
                ident,
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

        // Three separate files, small enough to bin-pack into one ScanTaskGroup, so a single
        // warmUp call must cover all of them.
        Object[][] rowsPerFile = {{1, "a"}, {2, "b"}, {3, "c"}};
        for (Object[] idAndData : rowsPerFile) {
          DataFile dataFile =
              FileHelpers.writeDataFile(
                  table, newLocalOutputFile("proactive-data"), rows(idAndData[0], idAndData[1]));
          table.newAppend().appendFile(dataFile).commit();

          Path source = toLocalPath(dataFile.location());
          Files.copy(
              source,
              servableFilesDir.resolve(source.getFileName()),
              StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        writeCatalog.close();
      }

      // Unsorted: an ORDER BY here would make Spark's range-partition sampling read this single
      // partition's files an extra time before the real shuffle, doubling the warmUp/presign
      // count for reasons unrelated to this test.
      List<Object[]> results =
          sql("SELECT id, data FROM %s", catalogName + ".default." + ident.name());
      results.sort(Comparator.comparingInt(r -> (Integer) r[0]));
      assertThat(results).containsExactly(row(1, "a"), row(2, "b"), row(3, "c"));
      assertThat(presignHits.get())
          .as("warmUp should batch-refresh every file in the task group in a single /presign call")
          .isEqualTo(1);
    } finally {
      presignCatalogServer.stop();
      backendCatalog.close();
    }
  }

  /**
   * Maps an already-planned file's stored location to a fresh presigned URL for a copy of that file
   * already placed in the Jetty-served directory, standing in for {@link
   * PresignedUrlVendingAdapter}'s presign function. Called both at plan time and again by {@code
   * HTTPFileIO#warmUp} via {@code /presign}, returning a distinct URL each time.
   */
  private String presignServedLocation(String location) {
    return presignedUrl(toLocalPath(location).getFileName().toString());
  }

  /**
   * Counts {@code POST .../presign} calls while delegating them to the adapter, standing in for the
   * catalog's {@code /presign} endpoint, which {@link RESTCatalogServlet} does not route.
   */
  private static final class PresignCountingServlet extends RESTCatalogServlet {

    private final PresignedUrlVendingAdapter adapter;
    private final AtomicInteger presignHits;

    PresignCountingServlet(PresignedUrlVendingAdapter adapter, AtomicInteger presignHits) {
      super(adapter);
      this.adapter = adapter;
      this.presignHits = presignHits;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      if (request.getRequestURI().endsWith("/presign")) {
        presignHits.incrementAndGet();
        handlePresign(request, response);
      } else {
        super.doPost(request, response);
      }
    }

    private void handlePresign(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      String body;
      try (Reader reader =
          new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8)) {
        body = CharStreams.toString(reader);
      }

      PresignResponse presignResponse = adapter.presign(PresignRequestParser.fromJson(body));
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("application/json");
      response.getWriter().write(PresignResponseParser.toJson(presignResponse));
    }
  }

  /**
   * Copies {@code dataFile} into the Jetty-served dir, rewrites its manifest location to the HTTP
   * URL, and deletes the original so a successful read can only have gone through {@link
   * HTTPFileIO}.
   */
  private DataFile relocateDataFile(Table table, DataFile dataFile) throws IOException {
    String newUrl = copyToServable(dataFile.location());
    DataFile relocated = DataFiles.builder(table.spec()).copy(dataFile).withPath(newUrl).build();

    table.newRewrite().deleteFile(dataFile).addFile(relocated).commit();

    deleteOriginalAndAssertGone(dataFile.location());
    return relocated;
  }

  /**
   * Same as {@link #relocateDataFile(Table, DataFile)}, but for a delete file (position deletes,
   * equality deletes, or a deletion vector).
   */
  private DeleteFile relocateDeleteFile(Table table, DeleteFile deleteFile) throws IOException {
    String newUrl = copyToServable(deleteFile.location());
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(table.spec());

    // FileMetadata.Builder#copy drops equality field IDs, so re-apply them (as
    // RewriteTablePathUtil#newEqualityDeleteEntry does).
    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      int[] equalityFieldIds =
          deleteFile.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
      builder.ofEqualityDeletes(equalityFieldIds);
    }

    DeleteFile relocated = builder.copy(deleteFile).withPath(newUrl).build();

    table.newRewrite().deleteFile(deleteFile).addFile(relocated).commit();

    deleteOriginalAndAssertGone(deleteFile.location());
    return relocated;
  }

  private String copyToServable(String originalLocation) throws IOException {
    Path source = toLocalPath(originalLocation);
    Path target = servableFilesDir.resolve(source.getFileName());
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    return presignedUrl(source.getFileName().toString());
  }

  private String presignedUrl(String fileName) {
    String base = httpServer.getURI().toString();
    String separator = base.endsWith("/") ? "" : "/";
    return base
        + separator
        + fileName
        + "?X-Mock-Signature="
        + UUID.randomUUID().toString().replace("-", "")
        + "&X-Mock-Expires="
        + Instant.now().plusSeconds(3600).getEpochSecond();
  }

  private void deleteOriginalAndAssertGone(String location) throws IOException {
    Path path = toLocalPath(location);
    Files.deleteIfExists(path);
    assertThat(Files.exists(path)).as("Original file should be deleted: %s", path).isFalse();
  }

  private static Path toLocalPath(String location) {
    return location.startsWith("file:")
        ? java.nio.file.Paths.get(URI.create(location))
        : java.nio.file.Paths.get(location);
  }

  private OutputFile newLocalOutputFile(String prefix) {
    String fileName = FileFormat.PARQUET.addExtension(prefix + "-" + UUID.randomUUID());
    return org.apache.iceberg.Files.localOutput(temp.resolve(fileName).toFile());
  }

  private static List<Record> rows(Object... idAndData) {
    Record template = GenericRecord.create(SCHEMA);
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < idAndData.length; i += 2) {
      records.add(template.copy("id", idAndData[i], "data", idAndData[i + 1]));
    }
    return records;
  }

  /**
   * Maps {@code count} ids to random, incompressible base64 values of {@code valueBytes} each, so
   * the Parquet file size scales predictably regardless of compression.
   */
  private static Map<Integer, String> wideRows(int count, int valueBytes) {
    Random random = new Random(0);
    Map<Integer, String> valuesById = Maps.newLinkedHashMap();
    for (int id = 0; id < count; id++) {
      byte[] value = new byte[valueBytes];
      random.nextBytes(value);
      valuesById.put(id, Base64.getEncoder().encodeToString(value));
    }
    return valuesById;
  }

  /**
   * Asserts a server-planned Spark scan's {@link FileIO} carries {@link
   * RESTCatalogProperties#REST_SCAN_PLAN_ID}, proving planning went through the REST endpoints
   * rather than reading the (relocated, partly deleted) manifests directly.
   */
  private void assertServerSidePlanningEngaged(TableIdentifier ident) throws IOException {
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    try {
      catalog.initialize(
          "presigned-url-planning-check",
          ImmutableMap.<String, String>builder()
              .putAll(restCatalog.properties())
              .put(
                  RESTCatalogProperties.SCAN_PLANNING_MODE,
                  RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
              .put(HTTPFileIO.ENABLED, "true")
              .build());
      Table table = catalog.loadTable(ident);

      SparkScanBuilder builder =
          new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
      Batch batch = builder.build().toBatch();
      FileIO fileIOForScan =
          (FileIO)
              assertThat(batch)
                  .extracting("fileIO")
                  .isInstanceOf(Supplier.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(Supplier.class))
                  .actual()
                  .get();
      // REST_SCAN_PLAN_ID proves planning went through REST; REST_SCAN_PRESIGN_ENDPOINT lets an
      // executor re-sign an expired URL.
      assertThat(fileIOForScan.properties())
          .containsKey(RESTCatalogProperties.REST_SCAN_PLAN_ID)
          .containsKey(RESTCatalogProperties.REST_SCAN_PRESIGN_ENDPOINT);
    } finally {
      catalog.close();
    }
  }
}
