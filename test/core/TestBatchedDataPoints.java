package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, IncomingDataPoints.class})
public class TestBatchedDataPoints {
  private static Config config;
  private static TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);

  @Before
  public void before() throws Exception {
    PowerMockito.whenNew(HBaseClient.class)
    .withArguments(anyString(), anyString()).thenReturn(client);
    config = new Config(false);
    tsdb = new TSDB(config);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    // mock UniqueId
    when(metrics.getId("foo")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getName(new byte[] { 0, 0, 1 })).thenReturn("foo");

    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getName(new byte[] { 0, 0, 1 })).thenReturn("host");
    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] { 0, 0, 1 });

    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getName(new byte[] { 0, 0, 1 })).thenReturn("web01");
    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    
    when(metrics.width()).thenReturn((short)1);
    when(tag_names.width()).thenReturn((short)1);
    when(tag_values.width()).thenReturn((short)1);
  }

  /**
   * Configures storage for the addPoint() tests to validate that we're storing
   * data points correctly.
   */
  @SuppressWarnings("unchecked")
  private void setupAddPointStorage() throws Exception {
    PowerMockito.mockStatic(IncomingDataPoints.class);   
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}; 
    PowerMockito.doAnswer(
        new Answer<byte[]>() {
          public byte[] answer(final InvocationOnMock unused) 
            throws Exception {
            return row;
          }
        }
    ).when(IncomingDataPoints.class, "rowKeyTemplate", (TSDB)any(), anyString(), 
        (Map<String, String>)any());
  }

  @Test
  public void timestamp() throws Exception {
    setupAddPointStorage();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    BatchedDataPoints bdp = new BatchedDataPoints(tsdb, "foo", tags);
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, 2);
    bdp.addPoint(1388534400750L, 2);

    assertEquals(1388534400000L, bdp.timestamp(0));
    assertEquals(1388534400500L, bdp.timestamp(1));
    assertEquals(1388534400750L, bdp.timestamp(2));
  }
  
  @Test
  public void isInteger() throws Exception {
    setupAddPointStorage();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    BatchedDataPoints bdp = new BatchedDataPoints(tsdb, "foo", tags);
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, Short.MIN_VALUE);
    bdp.addPoint(1388534401L, Integer.MIN_VALUE);
    bdp.addPoint(1388534401750L, 2.0f);
    
    assertTrue(bdp.isInteger(0));
    assertTrue(bdp.isInteger(1));
    assertTrue(bdp.isInteger(2));
    assertFalse(bdp.isInteger(3));
  }
  
  @Test
  public void longValue() throws Exception {
    setupAddPointStorage();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    BatchedDataPoints bdp = new BatchedDataPoints(tsdb, "foo", tags);
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, Short.MIN_VALUE);
    bdp.addPoint(1388534401L, Integer.MIN_VALUE);
    bdp.addPoint(1388534401750L, 2.0f);
    
    assertEquals(1, bdp.longValue(0));
    assertEquals(Short.MIN_VALUE, bdp.longValue(1));
    assertEquals(Integer.MIN_VALUE, bdp.longValue(2));
  }
  
  @Test
  public void doubleValue() throws Exception {
    setupAddPointStorage();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    BatchedDataPoints bdp = new BatchedDataPoints(tsdb, "foo", tags);
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, Short.MIN_VALUE);
    bdp.addPoint(1388534401L, Integer.MIN_VALUE);
    bdp.addPoint(1388534401750L, 2.0f);
    
    Assert.assertEquals(2.0f, bdp.doubleValue(3), 0.00001f);
  }

}
