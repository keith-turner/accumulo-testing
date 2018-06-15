package org.apache.accumulo.testing.core.performance.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.apache.accumulo.testing.core.performance.Metric;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Result;
import org.apache.accumulo.testing.core.performance.SystemConfiguration;
import org.apache.accumulo.testing.core.performance.util.TestData;
import org.apache.hadoop.io.Text;

public class ScanExecutorPT implements PerformanceTest {

  private static final String TEST_DESC = "Scan Executor Test.  Test running lots of short scans while many concurrent background scans are running.  A scan prioritizer that favors short scans is configured.";

  @Override
  public SystemConfiguration getConfiguration() {
    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "200");
    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "200");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.threads", "2");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.prioritizer",
       IdleRatioScanPrioritizer.class.getName());

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Result runTest(Environment env) throws Exception {

    String tableName = "scept";

    Map<String, String> props = new HashMap<>();
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey()+"executor","se1");

    env.getConnector().tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

    TestData.generate(env.getConnector(), tableName, 10000, 10, 10);

    env.getConnector().tableOperations().compact(tableName, null, null, true, true);

    AtomicBoolean stop = new AtomicBoolean(false);

    ExecutorService longScans = startLongScans(env, tableName, stop);

    double mean1 = runShortScans(env, tableName);
    double mean2 = runShortScans(env, tableName);
    double mean3 = runShortScans(env, tableName);

    stop.set(true);
    longScans.shutdown();

    return new Result(TEST_DESC, new Metric("run1", mean1, "Average scan times in ms for 1st batch of scans"), new Metric("run2", mean2, "Average scan times in ms for 2nd batch of scans"), new Metric("run3", mean3, "Average scan times in ms for 3rd batch of scans"));
  }

  private static long scan(String tableName, Connector c, byte[] row, byte[] fam) {
    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(new Text(row), new Text(fam)));
      for (Entry<Key,Value> entry : scanner) {
        count++;
      }
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }

    return System.currentTimeMillis() - t1;
  }

  private long scan(String tableName, Connector c, AtomicBoolean stop) {
    long count = 0;
    while (!stop.get()) {
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          count++;
          if (stop.get()) {
            return count;
          }
        }
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    return count;
  }

  private double runShortScans(Environment env, String tableName) throws InterruptedException, ExecutionException {
    int numLookups = 5000;
    ExecutorService shortScans = Executors.newFixedThreadPool(5);

    List<Future<Long>> futures = new ArrayList<>();

    Random rand = new Random();

    for (int i = 0; i < numLookups; i++) {
      byte[] row = TestData.row(rand.nextInt(10000));
      byte[] fam = TestData.fam(rand.nextInt(10));
      futures.add(shortScans.submit(() -> scan(tableName, env.getConnector(), row, fam)));
    }

    long sum = 0;
    for (Future<Long> future : futures) {
      sum += future.get();
    }

    shortScans.shutdown();

    double mean = sum /(double)numLookups;
    return mean;
  }

  private ExecutorService startLongScans(Environment env, String tableName, AtomicBoolean stop) {
    int numLongScans = 50;
    ExecutorService longScans = Executors.newFixedThreadPool(numLongScans);

    for (int i = 0; i < numLongScans; i++) {
      longScans.execute(() -> scan(tableName, env.getConnector(), stop));
    }
    return longScans;
  }

}
