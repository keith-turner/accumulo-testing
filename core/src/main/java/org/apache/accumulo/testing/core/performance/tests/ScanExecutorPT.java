package org.apache.accumulo.testing.core.performance.tests;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.IdleRatioScanPrioritizer;
import org.apache.accumulo.testing.core.performance.Environment;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Results;
import org.apache.accumulo.testing.core.performance.SystemConfiguration;
import org.apache.accumulo.testing.core.performance.util.TestData;
import org.apache.accumulo.testing.core.performance.util.TestExecutor;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

public class ScanExecutorPT implements PerformanceTest {

  private static final int NUM_SHORT_SCANS = 50000;
  private static final int NUM_SHORT_SCANS_THREADS = 5;
  private static final int NUM_LONG_SCANS = 50;

  private static final int NUM_ROWS = 10000;
  private static final int NUM_FAMS = 10;
  private static final int NUM_QUALS = 10;

  private static final String SCAN_EXECUTOR_THREADS = "2";
  private static final String SCAN_PRIORITIZER = IdleRatioScanPrioritizer.class.getName();

  private static final String TEST_DESC = "Scan Executor Test.  Test running lots of short scans "
      + "while long scans are running in the background.  A scan prioritizer that favors short "
      + "scans is configured.  If the scan prioritizer is not working properly, then the short " + "scans will be orders of magnitude slower.";

  @Override
  public SystemConfiguration getConfiguration() {
    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "200");
    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "200");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.threads", SCAN_EXECUTOR_THREADS);
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.prioritizer", SCAN_PRIORITIZER);

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Results runTest(Environment env) throws Exception {

    String tableName = "scept";

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey() + "executor", "se1");

    env.getConnector().tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

    TestData.generate(env.getConnector(), tableName, NUM_ROWS, NUM_FAMS, NUM_QUALS);

    env.getConnector().tableOperations().compact(tableName, null, null, true, true);

    AtomicBoolean stop = new AtomicBoolean(false);

    long t1 = System.currentTimeMillis();

    TestExecutor<Long> longScans = startLongScans(env, tableName, stop);

    LongSummaryStatistics shortStats1 = runShortScans(env, tableName);
    LongSummaryStatistics shortStats2 = runShortScans(env, tableName);

    stop.set(true);
    long t2 = System.currentTimeMillis();

    LongSummaryStatistics longStats = longScans.stream().mapToLong(l -> l).summaryStatistics();

    longScans.close();

    return Results.builder().description(TEST_DESC).info("short_times1", shortStats1, "Times in ms for each short scan.  First run.")
        .info("short_times2", shortStats2, "Times in ms for each short scan. Second run.")
        .result("short_avg2", shortStats2.getAverage(), "Average times in ms for short scans from 2nd run.")
        .info("long_counts", longStats, "Entries read by each long scan threads")
        .result("long_rate", longStats.getSum() / ((t2-t1)/1000.0), "Combined rate in entries/second of all long scans")
        .parameter("short_scans", NUM_SHORT_SCANS, "Short scans ran.  Each short scan reads a random row and family.")
        .parameter("short_threads", NUM_SHORT_SCANS_THREADS, "Threads used to run short scans.")
        .parameter("long_threads", NUM_LONG_SCANS, "Threads running long scans.  Each thread repeatedly scans entire table for duration of test.")
        .parameter("rows", NUM_ROWS, "Rows in test table").parameter("familes", NUM_FAMS, "Families per row in test table")
        .parameter("qualifiers", NUM_QUALS, "Qualifiers per family in test table")
        .parameter("server_scan_threads", SCAN_EXECUTOR_THREADS, "Server side scan handler threads")
        .parameter("prioritizer", SCAN_PRIORITIZER, "Server side scan prioritizer").build();
  }

  private static long scan(String tableName, Connector c, byte[] row, byte[] fam) throws TableNotFoundException {
    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(new Text(row), new Text(fam)));
      if (Iterables.size(scanner) != NUM_QUALS) {
        throw new RuntimeException("bad count " + count);
      }
    }

    return System.currentTimeMillis() - t1;
  }

  private long scan(String tableName, Connector c, AtomicBoolean stop) throws TableNotFoundException {
    long count = 0;
    while (!stop.get()) {
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          count++;
          if (stop.get()) {
            return count;
          }
        }
      }
    }
    return count;
  }

  private LongSummaryStatistics runShortScans(Environment env, String tableName) throws InterruptedException, ExecutionException {

    try(TestExecutor<Long> executor = new TestExecutor<>(NUM_SHORT_SCANS_THREADS)) {
      Random rand = new Random();

      for (int i = 0; i < NUM_SHORT_SCANS; i++) {
        byte[] row = TestData.row(rand.nextInt(NUM_ROWS));
        byte[] fam = TestData.fam(rand.nextInt(NUM_FAMS));
        executor.submit(() -> scan(tableName, env.getConnector(), row, fam));
      }

      return executor.stream().mapToLong(l -> l).summaryStatistics();
    }
  }

  private TestExecutor<Long> startLongScans(Environment env, String tableName, AtomicBoolean stop) {
    TestExecutor<Long> longScans = new TestExecutor<>(NUM_LONG_SCANS);

    for (int i = 0; i < NUM_LONG_SCANS; i++) {
      longScans.submit(() -> scan(tableName, env.getConnector(), stop));
    }
    return longScans;
  }
}
