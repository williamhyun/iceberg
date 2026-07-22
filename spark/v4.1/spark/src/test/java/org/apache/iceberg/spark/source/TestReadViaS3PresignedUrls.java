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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.http.HTTPFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.PresignedUrlVendingAdapter;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.requests.PresignRequest;
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
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

/**
 * Real-S3 integration test proving a REST catalog can vend S3 presigned URLs alongside a completed
 * scan plan (via {@link PresignedUrlVendingAdapter}) and that Spark's server-side-planning client
 * reads through them, including splitting large files into multiple {@code ScanTaskGroup}s.
 *
 * <p>The table is ordinary -- manifests, data files, and delete files all keep plain {@code s3://}
 * locations throughout; only the {@code pre-signed-urls} map on the plan response carries the
 * ephemeral HTTP URLs, which {@code HTTPFileIO} consults to resolve a location to bytes. Unlike
 * {@link TestReadViaPresignedUrls}, which permanently rewrites manifests to (mock) HTTP locations.
 */
@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = "AWS_REGION", matches = ".+"),
  @EnabledIfEnvironmentVariable(named = "AWS_TEST_BUCKET", matches = ".+")
})
public class TestReadViaS3PresignedUrls extends TestBaseWithCatalog {

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

  private static String bucket;
  private static String warehouseLocation;
  private static S3Client s3;
  private static S3Presigner presigner;
  private static Catalog backendCatalog;
  private static Server httpServer;
  private static URI serverUri;
  private static RESTCatalog writeCatalog;

  @BeforeAll
  public static void startServer() throws Exception {
    Region region = Region.of(System.getenv("AWS_REGION"));
    bucket = System.getenv("AWS_TEST_BUCKET");
    warehouseLocation = String.format("s3://%s/%s", bucket, UUID.randomUUID());

    s3 = AwsClientFactories.defaultFactory().s3();
    presigner =
        S3Presigner.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .build();

    Map<String, String> catalogProperties =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName())
            .put(CatalogProperties.URI, "jdbc:sqlite::memory:")
            .put("jdbc.schema-version", "V1")
            // in-memory sqlite is private to its connection; pin the pool to one connection
            .put(CatalogProperties.CLIENT_POOL_SIZE, "1")
            .put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation)
            // use S3FileIO directly instead of the JdbcCatalog default (HadoopFileIO + S3A)
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
            .buildOrThrow();
    backendCatalog =
        CatalogUtil.buildIcebergCatalog(
            "presigned_url_vending_backend", catalogProperties, new Configuration());

    RESTCatalogAdapter adapter =
        new PresignedUrlVendingAdapter(backendCatalog, TestReadViaS3PresignedUrls::presign);
    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(servlet), "/*");

    httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(context);
    httpServer.start();
    serverUri = httpServer.getURI();

    writeCatalog = new RESTCatalog();
    writeCatalog.setConf(new Configuration());
    writeCatalog.initialize(
        "presigned-url-vending-write-client",
        ImmutableMap.of(CatalogProperties.URI, serverUri.toString()));
  }

  @AfterAll
  public static void stopServer() throws Exception {
    if (writeCatalog != null) {
      writeCatalog.close();
    }
    if (httpServer != null) {
      httpServer.stop();
    }
    if (backendCatalog instanceof Closeable closeable) {
      closeable.close();
    }
    if (s3 != null) {
      deleteRecursively(warehouseLocation);
      s3.close();
    }
    if (presigner != null) {
      presigner.close();
    }
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.<String, String>builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, serverUri.toString())
            .put(
                RESTCatalogProperties.SCAN_PLANNING_MODE,
                RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
            .put(HTTPFileIO.ENABLED, "true")
            .build()
      }
    };
  }

  @TestTemplate
  public void readWithPositionDeletesViaS3PresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "position_deletes_table");
    Table table =
        writeCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, newS3OutputFile(table, "data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(Pair.of(dataFile.location(), 0L)); // delete id = 1
    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, newS3OutputFile(table, "pos-deletes"), null, deletes, 2);
    table.newRowDelta().addDeletes(posDeletes.first()).commit();

    assertServerSidePlanningEngaged(ident);

    List<Object[]> results = sql("SELECT id, data FROM %s ORDER BY id", tableName(ident.name()));
    assertThat(results).containsExactly(row(2, "b"), row(3, "c"));
  }

  @TestTemplate
  public void readWithEqualityDeletesViaS3PresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "equality_deletes_table");
    Table table =
        writeCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, newS3OutputFile(table, "data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    Schema deleteRowSchema = SCHEMA.select("id");
    Record deleteRecord = GenericRecord.create(deleteRowSchema);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            newS3OutputFile(table, "eq-deletes"),
            Lists.newArrayList(deleteRecord.copy("id", 2)), // delete id = 2
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    assertServerSidePlanningEngaged(ident);

    List<Object[]> results = sql("SELECT id, data FROM %s ORDER BY id", tableName(ident.name()));
    assertThat(results).containsExactly(row(1, "a"), row(3, "c"));
  }

  @TestTemplate
  public void readWithDeletionVectorsViaS3PresignedUrls() throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "dv_table");
    Table table =
        writeCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, newS3OutputFile(table, "data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(Pair.of(dataFile.location(), 0L)); // delete id = 1
    // formatVersion 3 makes FileHelpers write a real Puffin DV; the OutputFile arg is unused.
    Pair<DeleteFile, CharSequenceSet> dv =
        FileHelpers.writeDeleteFile(table, null, null, deletes, 3);
    table.newRowDelta().addDeletes(dv.first()).commit();

    assertServerSidePlanningEngaged(ident);

    List<Object[]> results = sql("SELECT id, data FROM %s ORDER BY id", tableName(ident.name()));
    assertThat(results).containsExactly(row(2, "b"), row(3, "c"));
  }

  /**
   * One large data file, planned as a single {@code FileScanTask}, is split client-side into
   * multiple {@code ScanTaskGroup}s that concurrently range-read the same presigned URL.
   */
  @TestTemplate
  public void readLargeDataFileAcrossConcurrentScanTaskGroupsViaS3PresignedUrls()
      throws IOException {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "large_file_table");
    Table table =
        writeCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    // Many small row groups + a matching small split target so the one data file below splits
    // into several concurrently-readable ScanTaskGroups.
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
    DataFile dataFile =
        FileHelpers.writeDataFile(table, newS3OutputFile(table, "large-data"), records);
    table.newAppend().appendFile(dataFile).commit();

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
   * End-to-end executor-side refresh against real S3: planning vends an already-expired presigned
   * URL, {@link RESTSignedUrlRefresherIntegrationServlet} stands in for the catalog's {@code
   * /presign} endpoint to mint a fresh one, and {@code HTTPFileIO} refreshes and retries
   * transparently. Uses a dedicated server since {@link RESTCatalogServlet} does not route {@code
   * POST .../presign}.
   */
  @TestTemplate
  public void refreshesExpiredPresignedUrlOnRead() throws Exception {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "refresh_table");
    Table table =
        writeCatalog.createTable(
            ident,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, newS3OutputFile(table, "data"), rows(1, "a", 2, "b", 3, "c"));
    table.newAppend().appendFile(dataFile).commit();

    PresignedUrlVendingAdapter adapter =
        new PresignedUrlVendingAdapter(
            backendCatalog, TestReadViaS3PresignedUrls::presignShortLived);
    RESTCatalogServlet servlet = new RESTSignedUrlRefresherIntegrationServlet(adapter);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(servlet), "/*");

    Server refreshServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    refreshServer.setHandler(context);
    refreshServer.start();
    try {
      RESTCatalog readCatalog = new RESTCatalog();
      readCatalog.setConf(new Configuration());
      readCatalog.initialize(
          "presigned-url-refresh-check",
          ImmutableMap.<String, String>builder()
              .put(CatalogProperties.URI, refreshServer.getURI().toString())
              .put(
                  RESTCatalogProperties.SCAN_PLANNING_MODE,
                  RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
              .put(HTTPFileIO.ENABLED, "true")
              .build());
      try {
        assertRefreshedReadSucceeds(readCatalog, ident);
      } finally {
        readCatalog.close();
      }
    } finally {
      refreshServer.stop();
    }
  }

  private void assertRefreshedReadSucceeds(RESTCatalog readCatalog, TableIdentifier ident)
      throws IOException, InterruptedException {
    Table readTable = readCatalog.loadTable(ident);
    TableScan tableScan = readTable.newScan();

    // The vended URL lives 1 second (presignShortLived); wait it out so the read is guaranteed to
    // hit a 403 and refresh rather than racing expiry.
    Thread.sleep(2000);

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> planned = tableScan.planFiles()) {
      tasks = Lists.newArrayList(planned);
    }
    assertThat(tasks).hasSize(1);

    FileIO scanIO = tableScan.fileIO().get();
    InputFile inputFile = scanIO.newInputFile(tasks.get(0).file().location());
    byte[] buffer = new byte[(int) inputFile.getLength()];
    try (SeekableInputStream in = inputFile.newStream()) {
      ((RangeReadable) in).readFully(0, buffer, 0, buffer.length);
    }

    // A non-empty read confirms the refreshed URL served real bytes.
    assertThat(buffer.length).isGreaterThan(0);
  }

  /**
   * A generous refresh threshold, combined with a 1ms {@code url-expiration-timestamp-ms} hint,
   * puts {@code HTTPFileIO#warmUp} inside its refresh window as soon as {@code BaseReader} calls it
   * -- before any read is attempted. The hint is independent of the actual S3 signature duration
   * (the default 1-hour duration from {@link #presign(String)}), so the refreshed URL always serves
   * real bytes.
   *
   * <p>The table has three small data files, all of which are bin-packed into a single {@code
   * ScanTaskGroup} (and therefore a single Spark partition), so a successful read with exactly one
   * {@code /presign} hit also proves {@code warmUp} re-signed every file this task needed in one
   * batch call rather than once per file.
   */
  @TestTemplate
  public void warmUpProactivelyRefreshesBeforeReadViaS3PresignedUrls() throws Exception {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("default"), "proactive_warm_up_table");
    AtomicInteger presignHits = new AtomicInteger();

    // A 1ms lifetime hint guarantees the vended URL is already inside any positive refresh
    // threshold by the time warmUp runs; the underlying S3 signature stays valid regardless.
    PresignedUrlVendingAdapter adapter =
        new PresignedUrlVendingAdapter(backendCatalog, TestReadViaS3PresignedUrls::presign, 1L);
    RESTCatalogServlet servlet = new PresignCountingServlet(adapter, presignHits);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(servlet), "/*");

    Server presignCatalogServer =
        new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    presignCatalogServer.setHandler(context);
    presignCatalogServer.start();
    try {
      String catalogUri = presignCatalogServer.getURI().toString();
      String catalogName = "proactive_s3_presign_catalog";
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

      RESTCatalog proactiveWriteCatalog = new RESTCatalog();
      proactiveWriteCatalog.setConf(new Configuration());
      proactiveWriteCatalog.initialize(
          "proactive-s3-warm-up-write", ImmutableMap.of(CatalogProperties.URI, catalogUri));
      Table table;
      try {
        table =
            proactiveWriteCatalog.createTable(
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
                  table,
                  newS3OutputFile(table, "proactive-data"),
                  rows(idAndData[0], idAndData[1]));
          table.newAppend().appendFile(dataFile).commit();
        }
      } finally {
        proactiveWriteCatalog.close();
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
    }
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
   * Mints a 1-second presigned URL to force the refresh path in {@link
   * #refreshesExpiredPresignedUrlOnRead()}.
   */
  private static String presignShortLived(String s3Location) {
    return presign(s3Location, Duration.ofSeconds(1));
  }

  /**
   * Stands in for the catalog's {@code /presign} endpoint, which {@link RESTCatalogServlet} does
   * not route: intercepts {@code POST .../presign} and delegates to the adapter to re-sign the
   * requested file paths with fresh, long-lived URLs, delegating everything else unchanged.
   */
  private static final class RESTSignedUrlRefresherIntegrationServlet extends RESTCatalogServlet {

    private final PresignedUrlVendingAdapter adapter;

    RESTSignedUrlRefresherIntegrationServlet(PresignedUrlVendingAdapter adapter) {
      super(adapter);
      this.adapter = adapter;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      String path = request.getRequestURI().substring(1);
      if (path.endsWith("/presign")) {
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

      PresignRequest presignRequest = PresignRequestParser.fromJson(body);
      PresignResponse presignResponse = adapter.presign(presignRequest);

      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("application/json");
      response.getWriter().write(PresignResponseParser.toJson(presignResponse));
    }
  }

  private static OutputFile newS3OutputFile(Table table, String prefix) {
    String location =
        String.format("%s/data/%s-%s.parquet", table.location(), prefix, UUID.randomUUID());
    return table.io().newOutputFile(location);
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

  private static String presign(String s3Location) {
    return presign(s3Location, Duration.ofHours(1));
  }

  private static String presign(String s3Location, Duration signatureDuration) {
    URI uri = URI.create(s3Location);
    String bucketName = uri.getHost();
    String key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
    PresignedGetObjectRequest presigned =
        presigner.presignGetObject(
            GetObjectPresignRequest.builder()
                .signatureDuration(signatureDuration)
                .getObjectRequest(r -> r.bucket(bucketName).key(key))
                .build());
    return presigned.url().toString();
  }

  private static void deleteRecursively(String location) {
    URI uri = URI.create(location);
    String key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();

    List<ObjectIdentifier> toDelete = Lists.newArrayList();
    for (S3Object object :
        s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(key).build())
            .contents()) {
      toDelete.add(ObjectIdentifier.builder().key(object.key()).build());
    }

    if (!toDelete.isEmpty()) {
      s3.deleteObjects(
          DeleteObjectsRequest.builder()
              .bucket(bucket)
              .delete(Delete.builder().objects(toDelete).build())
              .build());
    }
  }

  /**
   * Asserts a server-planned scan populates a presigned S3 URL for every planned file's (plain
   * {@code s3://}) location in the {@code HTTPFileIO} consulted for reads. Only {@link
   * org.apache.iceberg.rest.PresignedUrlVendingAdapter} populates that map, so this proves planning
   * went through the REST server rather than falling back to reading manifests directly.
   */
  private void assertServerSidePlanningEngaged(TableIdentifier ident) throws IOException {
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    try {
      catalog.initialize(
          "presigned-url-planning-check",
          ImmutableMap.<String, String>builder()
              .put(CatalogProperties.URI, serverUri.toString())
              .put(
                  RESTCatalogProperties.SCAN_PLANNING_MODE,
                  RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
              .put(HTTPFileIO.ENABLED, "true")
              .build());
      Table table = catalog.loadTable(ident);
      TableScan tableScan = table.newScan();

      List<FileScanTask> tasks;
      try (CloseableIterable<FileScanTask> planned = tableScan.planFiles()) {
        tasks = Lists.newArrayList(planned);
      }
      assertThat(tasks).isNotEmpty();

      FileIO scanIO = tableScan.fileIO().get();
      assertThat(scanIO).isInstanceOf(HTTPFileIO.class);
      Map<String, String> preSignedUrls = ((HTTPFileIO) scanIO).preSignedUrls();

      for (FileScanTask task : tasks) {
        assertThat(preSignedUrls)
            .as("data file location should have a presigned S3 URL")
            .hasEntrySatisfying(
                task.file().location(),
                url -> assertThat(url).startsWith("https://").contains("X-Amz-Signature="));
      }
    } finally {
      catalog.close();
    }
  }
}
