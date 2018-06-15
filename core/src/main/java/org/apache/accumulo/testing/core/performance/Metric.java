package org.apache.accumulo.testing.core.performance;

public class Metric {

  public final String id;
  public final Number data;
  public final String description;

  public Metric(String id, Number data, String description) {
    this.id = id;
    this.data = data;
    this.description = description;
  }

  public Metric(String id, Number data) {
    this.id = id;
    this.data = data;
    this.description = "";
  }

}
