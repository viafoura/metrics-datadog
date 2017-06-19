package org.coursera.metrics.datadog.transport;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.validation.constraints.NotNull;

@JsonTypeName("udp")
public class UdpTransportFactory implements AbstractTransportFactory {

  @NotNull
  @JsonProperty
  private String statsdHost = "localhost";

  @JsonProperty
  private int port = 8125;

  @JsonProperty
  private boolean retryingLookup = false;

  @JsonProperty
  private String prefix = null;

  public UdpTransport build() {
    return new UdpTransport.Builder()
        .withPrefix(prefix)
        .withStatsdHost(statsdHost)
        .withPort(port)
        .withRetryingLookup(retryingLookup)
        .build();
    }
}
