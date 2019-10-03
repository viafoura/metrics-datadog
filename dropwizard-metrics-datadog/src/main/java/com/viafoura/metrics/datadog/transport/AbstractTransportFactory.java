package com.viafoura.metrics.datadog.transport;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface AbstractTransportFactory extends Discoverable {
  public Transport build();
}
