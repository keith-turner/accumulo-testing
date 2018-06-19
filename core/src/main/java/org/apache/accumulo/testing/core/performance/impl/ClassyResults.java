package org.apache.accumulo.testing.core.performance.impl;

import org.apache.accumulo.testing.core.performance.Results;

public class ClassyResults extends Results {

  public final String testClass;

  public ClassyResults(String testClass, Results r) {
    super(r.description, r.results, r.parameters);
    this.testClass = testClass;
  }

}
