package org.apache.aurora.scheduler.mesos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Test;

public class TrackableResourceTest {
	private static final Resource scalarResource = Resource.newBuilder().setType(Type.SCALAR)
	    .setName(Resources.CPUS).setScalar(Scalar.newBuilder().setValue(10)).setRole(Resources.MESOS_DEFAULT_ROLE).build();
	
	private static final Resource singleRangeResource = Resource
	    .newBuilder()
	    .setType(Type.RANGES)
	    .setName(Resources.PORTS)
	    .setRole(Resources.MESOS_DEFAULT_ROLE)
	    .setRanges(
	        Ranges.newBuilder().addRange(Range.newBuilder().setBegin(1000).setEnd(1000).build()))
	    .build();
	
	private static final Resource multipleRangeResource = Resource
	    .newBuilder()
	    .setType(Type.RANGES)
	    .setName(Resources.PORTS)
	    .setRole(Resources.MESOS_DEFAULT_ROLE)
	    .setRanges(
	        Ranges.newBuilder().addRange(Range.newBuilder().setBegin(1000).setEnd(1000).build())
	            .addRange(Range.newBuilder().setBegin(1001).setEnd(1001).build())).build();
	

	@Test
	public void testAallocateFromScalar() {
		TrackableResource trackableResource = new TrackableResource(scalarResource);
		double allocated = trackableResource.allocateFromScalar(4.9);
		assertEquals(4.9, allocated, 0);

		allocated = trackableResource.allocateFromScalar(10);
		assertEquals(5.1, allocated, 0);
	}

	@Test
	public void testAllocateFromRange() {
		TrackableResource trackableResource = new TrackableResource(singleRangeResource);
		Optional<Long> allocated = trackableResource.allocateFromRange();
		assertEquals(allocated.orNull().intValue(), 1000);
		
		allocated = trackableResource.allocateFromRange();
		assertFalse(allocated.isPresent());
	}
	
	@Test
	public void testAllocateFromRangeWithMultipleRange() {
		TrackableResource trackableResource = new TrackableResource(multipleRangeResource);
		Optional<Long> allocated = trackableResource.allocateFromRange();
		assertTrue(allocated.isPresent());
		
		allocated = trackableResource.allocateFromRange();
		assertTrue(allocated.isPresent());
		
		allocated = trackableResource.allocateFromRange();
		assertFalse(allocated.isPresent());
	}	
}
