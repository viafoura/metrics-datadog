package com.viafoura.metrics.datadog;

public interface MetricNameFormatter {

  public String format(String name, String... path);
}
