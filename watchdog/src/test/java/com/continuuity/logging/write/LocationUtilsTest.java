package com.continuuity.logging.write;

import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test LocationUtils.
 */
public class LocationUtilsTest {
  private final LocationFactory locationFactory = new LocalLocationFactory();

  @Test
  public void testNormalize() throws Exception {
    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/")).toURI());
    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("//")).toURI());

    Assert.assertEquals(locationFactory.create("/abc").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc")).toURI());
    Assert.assertEquals(locationFactory.create("/abc").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc/")).toURI());
    Assert.assertEquals(locationFactory.create("/abc").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc//")).toURI());

    Assert.assertEquals(locationFactory.create("/abc/def").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc/def")).toURI());
    Assert.assertEquals(locationFactory.create("/abc/def").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc/def/")).toURI());
    Assert.assertEquals(locationFactory.create("/abc/def").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc/def//")).toURI());
    Assert.assertEquals(locationFactory.create("/abc/def").toURI(),
                        LocationUtils.normalize(locationFactory, locationFactory.create("/abc/def///")).toURI());
  }

  @Test
  public void testGetParent() throws Exception {
    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.getParent(locationFactory.create("/")).toURI());
    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.getParent(locationFactory.create("//")).toURI());



    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.getParent(locationFactory.create("/tmp")).toURI());
    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.getParent(locationFactory.create("/tmp/")).toURI());
    Assert.assertEquals(locationFactory.create("/").toURI(),
                        LocationUtils.getParent(locationFactory.create("/tmp//")).toURI());

    Assert.assertEquals(locationFactory.create("/tmp").toURI(),
                        LocationUtils.getParent(locationFactory.create("/tmp/a")).toURI());
    Assert.assertEquals(locationFactory.create("/tmp").toURI(),
                        LocationUtils.getParent(locationFactory.create("/tmp/a/")).toURI());
    Assert.assertEquals(locationFactory.create("/tmp").toURI(),
                        LocationUtils.getParent(locationFactory.create("/tmp/a///")).toURI());


  }
}
