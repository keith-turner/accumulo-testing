package org.apache.accumulo.testing.core.performance.impl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.testing.core.performance.Environment;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Report;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MiniPerfTestRunner {
  public static void main(String[] args) throws Exception {

    FileSystem.get(new Configuration());

    String className = args[0];
    String accumuloVersion = args[1];
    Path dir = Files.createTempDirectory(Paths.get("/tmp"), "accumulo-perf-test");

    PerformanceTest perfTest = Class.forName(className).asSubclass(PerformanceTest.class).newInstance();

    MiniAccumuloConfig cfg = new MiniAccumuloConfig(dir.toFile(), "secret");
    dir.toFile().deleteOnExit();

    cfg.setSiteConfig(perfTest.getConfiguration().getAccumuloSite());

    MiniAccumuloCluster mac = new MiniAccumuloCluster(cfg);
    mac.start();

    Instant start = Instant.now();

    Report result = perfTest.runTest(new Environment() {
      @Override
      public Connector getConnector() {
        try {
          return mac.getConnector("root", "secret");
        } catch (AccumuloException | AccumuloSecurityException e) {
          throw new RuntimeException(e);
        }
      }
    });

    Instant stop = Instant.now();

    mac.stop();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    System.out.println(gson.toJson(new ContextualReport(className, accumuloVersion, start, stop, result)));
  }
}
