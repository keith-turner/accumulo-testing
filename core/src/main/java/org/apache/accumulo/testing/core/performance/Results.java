package org.apache.accumulo.testing.core.performance;

import java.util.List;
import java.util.LongSummaryStatistics;

import org.apache.accumulo.testing.core.performance.Result.Purpose;

import com.google.common.collect.ImmutableList;

public class Results {
  public final String description;
  public final List<Result> results;
  public final List<Parameter> parameters;

  public Results(String description, List<Result> results, List<Parameter> parameters) {
    this.description = description;
    this.results = ImmutableList.copyOf(results);
    this.parameters = ImmutableList.copyOf(parameters);
  }

  public static class Builder {
    private String description = "";
    private final ImmutableList.Builder<Result> results = new ImmutableList.Builder<>();
    private final ImmutableList.Builder<Parameter> parameters = new ImmutableList.Builder<>();

    private Builder() {}

    public Builder description(String desc) {
      this.description = desc;
      return this;
    }

    public Builder result(String id, LongSummaryStatistics stats, String description) {
      results.add(new Result(id, new Stats(stats.getMin(), stats.getMax(), stats.getSum(), stats.getAverage(), stats.getCount()), description,
          Purpose.COMPARISON));
      return this;
    }

    public Builder result(String id, Number data, String description) {
      results.add(new Result(id, data, description, Purpose.COMPARISON));
      return this;
    }

    public Builder info(String id, LongSummaryStatistics stats, String description) {
      results.add(new Result(id, new Stats(stats.getMin(), stats.getMax(), stats.getSum(), stats.getAverage(), stats.getCount()), description,
          Purpose.INFORMATIONAL));
      return this;
    }

    public Builder info(String id, Number data, String description) {
      results.add(new Result(id, data, description, Purpose.INFORMATIONAL));
      return this;
    }

    public Builder parameter(String id, Number data, String description) {
      parameters.add(new Parameter(id, data.toString(), description));
      return this;
    }

    public Builder parameter(String id, String data, String description) {
      parameters.add(new Parameter(id, data, description));
      return this;
    }

    public Results build() {
      return new Results(description, results.build(), parameters.build());
    }
  }

  public static Builder builder() {
    return new Builder();
  }

}
