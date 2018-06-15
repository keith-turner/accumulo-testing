package org.apache.accumulo.testing.core.performance;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class Result {
  public final String description;
  public final List<Metric> metrics;

  public Result(String description, Metric ... metrics) {
    this.description = description;
    this.metrics = ImmutableList.copyOf(metrics);
  }

  public Result(String description, List<Metric> metrics) {
    this.description = description;
    this.metrics = ImmutableList.copyOf(metrics);
  }
}
