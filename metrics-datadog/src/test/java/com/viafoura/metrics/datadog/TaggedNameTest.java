package com.viafoura.metrics.datadog;

import org.junit.Test;

import static com.viafoura.metrics.datadog.TaggedName.TaggedNameBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TaggedNameTest {

  @Test
  public void testBuildNoTags() throws Exception {
    TaggedName taggedName = new TaggedNameBuilder()
        .metricName("metric")
        .build();

    assertEquals("metric", taggedName.getMetricName());
    assertEquals(0, taggedName.getEncodedTags().size());
    assertEquals("metric", taggedName.encode());
  }

  @Test
  public void testBuildOneTag() throws Exception {
    TaggedName taggedName = new TaggedNameBuilder()
        .metricName("metric")
        .addTag("key1:val1")
        .build();

    assertEquals("metric", taggedName.getMetricName());
    assertEquals(1, taggedName.getEncodedTags().size());
    assertEquals("key1:val1", taggedName.getEncodedTags().get(0));

    assertEquals("metric[key1:val1]", taggedName.encode());
  }

  @Test
  public void testBuildManyTags() throws Exception {
    TaggedName taggedName = new TaggedNameBuilder()
        .metricName("metric")
        .addTag("key1", "val1")
        .addTag("key2", "val2")
        .build();

    assertEquals("metric", taggedName.getMetricName());
    assertEquals(2, taggedName.getEncodedTags().size());
    assertEquals("key1:val1", taggedName.getEncodedTags().get(0));
    assertEquals("key2:val2", taggedName.getEncodedTags().get(1));

    assertEquals("metric[key1:val1,key2:val2]", taggedName.encode());
  }

  @Test
  public void testDecodeNoTags() throws Exception {
    TaggedName tn = TaggedName.decode("metric");
    assertEquals("metric", tn.getMetricName());
    assertEquals(0, tn.getEncodedTags().size());
  }

  @Test
  public void testDecodeOneTag() throws Exception {
    TaggedName tn = TaggedName.decode("metric[key]");
    assertEquals("metric", tn.getMetricName());
    assertEquals(1, tn.getEncodedTags().size());
    assertEquals("key", tn.getEncodedTags().get(0));

  }

  @Test
  public void testDecodeTwoTags() throws Exception {
    TaggedName tn = TaggedName.decode("metric[key1:val1,key2]");
    assertEquals("metric", tn.getMetricName());
    assertEquals(2, tn.getEncodedTags().size());
    assertEquals("key1:val1", tn.getEncodedTags().get(0));
    assertEquals("key2", tn.getEncodedTags().get(1));
  }

  @Test
  public void testDecodeTwoTagsDotted() throws Exception {
    TaggedName tn = TaggedName.decode("my.metric.name[key.1:val.1,key.2]");
    assertEquals("my.metric.name", tn.getMetricName());
    assertEquals(2, tn.getEncodedTags().size());
    assertEquals("key.1:val.1", tn.getEncodedTags().get(0));
    assertEquals("key.2", tn.getEncodedTags().get(1));
  }

  @Test
  public void testDecodeTwoTagsDashed() throws Exception {
    TaggedName tn = TaggedName.decode("my-metric-name[key-1:val-1,key-2]");
    assertEquals("my-metric-name", tn.getMetricName());
    assertEquals(2, tn.getEncodedTags().size());
    assertEquals("key-1:val-1", tn.getEncodedTags().get(0));
    assertEquals("key-2", tn.getEncodedTags().get(1));
  }

  @Test
  public void testDecodeTwoTagsUnderscored() throws Exception {
    TaggedName tn = TaggedName.decode("my_metric_name[key_1:val_1,key_2]");
    assertEquals("my_metric_name", tn.getMetricName());
    assertEquals(2, tn.getEncodedTags().size());
    assertEquals("key_1:val_1", tn.getEncodedTags().get(0));
    assertEquals("key_2", tn.getEncodedTags().get(1));
  }

  @Test
  public void testDecodeInvalidEncodings() throws Exception {
    // note that parsing could be stricter, but we're relaxing things to
    // maintain backward compatibility with prior parsing logic
    assertInvalidEncoding(null);
    assertInvalidEncoding("");
    assertInvalidEncoding("metric[ ,]");
    assertInvalidEncoding("metric[tag, ]");
    assertInvalidEncoding("metric[,tag]");
    assertInvalidEncoding("metric[tag,,tag]");
  }

  private void assertInvalidEncoding(String encodedTagName) {
    try {
      TaggedName n = TaggedName.decode(encodedTagName);
      System.out.println(n.getMetricName() + ": " + n.getEncodedTags().size());
      fail("Expected decoding exception");
    } catch (Exception e) {
      // ok
    }
  }
}
