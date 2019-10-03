package com.viafoura.metrics.datadog;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import org.fest.assertions.api.Assertions;
import org.junit.Test;

public class DefaultMetricNameFormatterFactoryTest {
  @Test
  public void isDiscoverable() throws Exception {
    Assertions
            .assertThat(new DiscoverableSubtypeResolver().getDiscoveredSubtypes())
            .contains(DefaultMetricNameFormatterFactory.class);
  }

  @Test
  public void testBuild() throws Exception {
    Assertions
            .assertThat(new DefaultMetricNameFormatterFactory().build())
            .isInstanceOf(DefaultMetricNameFormatter.class);
  }
}
