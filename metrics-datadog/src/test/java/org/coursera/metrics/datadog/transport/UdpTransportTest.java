package org.coursera.metrics.datadog.transport;

import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.dcm.DnsCacheManipulator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class UdpTransportTest {
  private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String TEST_HOST = "fastandunresolvable";
  private static final int TEST_PORT = 1111;

  @Before
  public void cleanDnsCache() {
    DnsCacheManipulator.clearDnsCache();
  }

  @Test
  public void constructsWithReachableHost() {
    DnsCacheManipulator.setDnsCache(TEST_HOST, LOCAL_IP);

    final UdpTransport transport = new UdpTransport.Builder().withStatsdHost(TEST_HOST).build();
    assertNotNull(transport);
  }

  @Test(expected = RuntimeException.class)
  public void constructsWhenUnreachableHostWithRetry() {
    assertNotNull(new UdpTransport.Builder().withStatsdHost(TEST_HOST).withRetryingLookup(true).build());
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenUnreachableHost() {
    new UdpTransport.Builder().withStatsdHost(TEST_HOST).build();
  }

  @Test
  public void volatileResolverResolvesByTheTimeTheHostIsResolvable() throws Exception {
    final Callable<SocketAddress> retryingCallable = UdpTransport.volatileAddressResolver(TEST_HOST, TEST_PORT);
    // ^ Doesn't crash when host is unresolvable.

    try {
      retryingCallable.call();
      assertFalse(true);
    } catch (final Exception e) {}
    // ^ This should throw becuase the host is unresolvable.

    DnsCacheManipulator.setDnsCache(TEST_HOST, LOCAL_IP); // Make host resolvable.
    assertNotNull(retryingCallable.call()); // Returns with resolved by the time it's resolvable.
  }
}
