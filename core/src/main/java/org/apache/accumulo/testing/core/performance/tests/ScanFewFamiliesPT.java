package org.apache.accumulo.testing.core.performance.tests;

import java.util.HashSet;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.performance.Environment;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Results;
import org.apache.accumulo.testing.core.performance.SystemConfiguration;
import org.apache.accumulo.testing.core.performance.util.TestData;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

public class ScanFewFamiliesPT implements PerformanceTest {

  private static final String DESC = "This test times fetching a few column famlies when rows have many column families.";

  private static final int NUM_ROWS = 100;
  private static final int NUM_FAMS = 10000;
  private static final int NUM_QUALS = 1;

  @Override
  public SystemConfiguration getConfiguration() {
    return new SystemConfiguration();
  }

  @Override
  public Results runTest(Environment env) throws Exception {

    String tableName = "bigFamily";

    env.getConnector().tableOperations().create(tableName);

    TestData.generate(env.getConnector(), tableName, NUM_ROWS, NUM_FAMS, NUM_QUALS);

    // warm up run
    runScans(env, tableName, 1);

    Results.Builder builder = Results.builder();

    for (int numFams : new int[] {1, 2, 4, 8, 16}) {
      LongSummaryStatistics stats = runScans(env, tableName, numFams);
      builder.result("fetch_" + numFams, stats, "Times in ms to fetch " + numFams + " families from all rows");
    }

    builder.description(DESC);
    builder.parameter("rows", NUM_ROWS, "Rows in test table");
    builder.parameter("familes", NUM_FAMS, "Families per row in test table");
    builder.parameter("qualifiers", NUM_QUALS, "Qualifiers per family in test table");

    return builder.build();
  }

  private LongSummaryStatistics runScans(Environment env, String tableName, int numFamilies) throws TableNotFoundException {
    Random rand = new Random();
    LongSummaryStatistics stats = new LongSummaryStatistics();
    for (int i = 0; i < 50; i++) {
      stats.accept(scan(tableName, env.getConnector(), rand, numFamilies));
    }
    return stats;
  }

  private static long scan(String tableName, Connector c, Random rand, int numFamilies) throws TableNotFoundException {

    Set<Text> families = new HashSet<>(numFamilies);
    while(families.size() < numFamilies) {
      families.add(new Text(TestData.fam(rand.nextInt(NUM_FAMS))));
    }

    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      families.forEach(scanner::fetchColumnFamily);
      if (Iterables.size(scanner) != NUM_ROWS * NUM_QUALS * numFamilies) {
        throw new RuntimeException("bad count " + count);
      }
    }

    return System.currentTimeMillis() - t1;
  }
}
