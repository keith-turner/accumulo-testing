package org.apache.accumulo.testing.core.performance;

public interface PerformanceTest {

  SystemConfiguration getConfiguration();

  Results runTest(Environment env) throws Exception;
}
