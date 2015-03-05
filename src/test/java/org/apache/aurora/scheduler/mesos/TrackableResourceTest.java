/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.mesos;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrackableResourceTest {
  private static final Resource SCALAR_RESOURCE = Resource.newBuilder().setType(Type.SCALAR)
      .setName(Resources.CPUS).setScalar(Scalar.newBuilder().setValue(10))
      .setRole(Resources.MESOS_DEFAULT_ROLE).build();

  private static final Resource SINGLE_RANGE_RESOURCE = Resource
      .newBuilder()
      .setType(Type.RANGES)
      .setName(Resources.PORTS)
      .setRole(Resources.MESOS_DEFAULT_ROLE)
      .setRanges(
          Ranges.newBuilder().addRange(Range.newBuilder().setBegin(1000).setEnd(1000).build()))
          .build();

  private static final Resource MULTIPLE_RANGE_RESOURCE = Resource
      .newBuilder()
      .setType(Type.RANGES)
      .setName(Resources.PORTS)
      .setRole(Resources.MESOS_DEFAULT_ROLE)
      .setRanges(
          Ranges.newBuilder().addRange(Range.newBuilder().setBegin(1000).setEnd(1000).build())
          .addRange(Range.newBuilder().setBegin(1001).setEnd(1001).build())).build();

  @Test
  public void testAallocateFromScalar() {
    TrackableResource trackableResource = new TrackableResource(SCALAR_RESOURCE);
    double allocated = trackableResource.allocateFromScalar(4.9);
    assertEquals(4.9, allocated, 0);

    allocated = trackableResource.allocateFromScalar(10);
    assertEquals(5.1, allocated, 0);
  }

  @Test
  public void testAllocateFromRange() {
    TrackableResource trackableResource = new TrackableResource(SINGLE_RANGE_RESOURCE);
    Optional<Long> allocated = trackableResource.allocateFromRange();
    assertEquals(allocated.orNull().intValue(), 1000);

    allocated = trackableResource.allocateFromRange();
    assertFalse(allocated.isPresent());
  }

  @Test
  public void testAllocateFromRangeWithMultipleRange() {
    TrackableResource trackableResource = new TrackableResource(MULTIPLE_RANGE_RESOURCE);
    Optional<Long> allocated = trackableResource.allocateFromRange();
    assertTrue(allocated.isPresent());

    allocated = trackableResource.allocateFromRange();
    assertTrue(allocated.isPresent());

    allocated = trackableResource.allocateFromRange();
    assertFalse(allocated.isPresent());
  }
}
