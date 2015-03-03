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

import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;

public class TrackableResource {
	private Resource resource;
	private double usedScalarResource = 0;
	private Set<String> usedSetResource = Sets.newHashSet();
	private Set<Long> usedRangeResource = Sets.newHashSet();

	public TrackableResource(Resource resource) {
		this.resource = resource;
	}

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

//	public boolean allocatable() {
//		if (resource.getType().equals(Type.SCALAR)) {
//			return resource.getScalar().getValue() > usedScalarResource;
//		} else if (resource.getType().equals(Type.RANGES)) {
//			return false;
//		} else if (resource.getType().equals(Type.SET)) {
//			return resource.getSet().getItemList().size() > usedSetResource.size();
//		}
//		return false;
//	}

	public double getAvailableScalarResources() {
		return resource.getScalar().getValue() - usedScalarResource;
	}

	public Set<Long> getAvailableRangeResources() {
		Set<Long> allocatedRangeSet = Sets.newHashSet(Iterables.concat(Iterables.transform(resource
		    .getRanges().getRangeList(), RANGE_TO_MEMBERS)));

		return Sets.symmetricDifference(allocatedRangeSet, usedRangeResource);
	}

	public Set<String> getAvailableSetResources() {
		Set<String> allocatedSet = Sets.newHashSet(resource.getSet().getItemList());
		return Sets.symmetricDifference(allocatedSet, usedSetResource);
	}

	// public void allocateFromScalar(double allocated) {
	// this.usedScalarResource += allocated;
	// }

	/**
	 * allocate a resource from range
	 * @param allocated
	 * @return return allocated resource, if resource available, return Optional.absent()
	 */
	public Optional<Long> allocateFromRange() {
		Set<Long> availableResources = getAvailableRangeResources();
		if(availableResources.isEmpty()) {
			return Optional.absent();
		}
		Iterator<Long> it = Iterables.consumingIterable(availableResources).iterator();
		Long allocated = it.next();
		this.usedRangeResource.add(allocated);
		return Optional.of(allocated);
	}

	/**
	 * allocate a resource from set
	 * @param allocated
	 * @return return allocated resource, if resource available, return Optional.absent()
	 */
	public Optional<String> allocateFromSet() {
		Set<String> availableResources = getAvailableSetResources();
		if(availableResources.isEmpty()) {
			return Optional.absent();
		}
		Iterator<String> it = Iterables.consumingIterable(availableResources).iterator();
		String allocated = it.next();
		this.usedSetResource.add(allocated);
		return Optional.of(allocated);
	}

	/**
	 * 
	 * @param required
	 * @return the resource actually allocated
	 */
	public double allocateFromScalar(double required) {
		double available = getAvailableScalarResources();
		double allocated = available > required ? required : available;
		usedScalarResource += allocated;
		return allocated;
	}


	private static final Function<Range, Set<Long>> RANGE_TO_MEMBERS = new Function<Range, Set<Long>>() {
		@Override
		public Set<Long> apply(Range range) {
			return ContiguousSet.create(
			    com.google.common.collect.Range.closed(range.getBegin(), range.getEnd()),
			    DiscreteDomain.longs());
		}
	};

	public String getRole() {
		return resource.getRole();
	}


}
