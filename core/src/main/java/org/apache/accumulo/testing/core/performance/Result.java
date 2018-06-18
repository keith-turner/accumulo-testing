package org.apache.accumulo.testing.core.performance;

public class Result {

  public final String id;
  public final Number data;
  public final Stats stats;
  public final String description;

  public Result(String id, Number data, String description) {
    this.id = id;
    this.data = data;
    this.stats = null;
    this.description = description;
  }

  public Result(String id, Stats stats, String description) {
    this.id = id;
    this.stats = stats;
    this.data = null;
    this.description = description;
  }
}
