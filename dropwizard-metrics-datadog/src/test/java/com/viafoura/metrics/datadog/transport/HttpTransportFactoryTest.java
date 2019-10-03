package com.viafoura.metrics.datadog.transport;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import org.fest.assertions.api.Assertions;
import org.junit.Test;

public class HttpTransportFactoryTest {
  @Test
  public void isDiscoverable() throws Exception {
    Assertions
            .assertThat(new DiscoverableSubtypeResolver().getDiscoveredSubtypes())
            .contains(HttpTransportFactory.class);
  }
}