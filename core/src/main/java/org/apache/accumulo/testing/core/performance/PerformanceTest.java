package org.apache.accumulo.testing.core.performance;

public interface PerformanceTest {

  SystemConfiguration getConfiguration();

  Result runTest(Environment env) throws Exception;
}
