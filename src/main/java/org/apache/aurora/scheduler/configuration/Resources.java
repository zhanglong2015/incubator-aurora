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
package org.apache.aurora.scheduler.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.async.ResourceContext;
import org.apache.aurora.scheduler.async.ResourceContextHolder;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.mesos.TrackableResource;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;

import static java.util.Objects.requireNonNull;

/**
 * A container for multiple resource vectors. TODO(wfarner): Collapse this in with
 * ResourceAggregates AURORA-105.
 */
public class Resources {
  private static final Logger LOG = Logger.getLogger(Resources.class.getName());
  public static final String CPUS = "cpus";
  public static final String RAM_MB = "mem";
  public static final String DISK_MB = "disk";
  public static final String PORTS = "ports";
  public static final String MESOS_DEFAULT_ROLE = "*";

  private static final Function<Range, Set<Integer>> RANGE_TO_MEMBERS =
      new Function<Range, Set<Integer>>() {
    @Override
    public Set<Integer> apply(Range range) {
      return ContiguousSet.create(
          com.google.common.collect.Range.closed((int) range.getBegin(), (int) range.getEnd()),
          DiscreteDomain.integers());
    }
  };
  private final double numCpus;
  private final Amount<Long, Data> disk;
  private final Amount<Long, Data> ram;
  private final int numPorts;

  /**
   * Creates a new resources object.
   *
   * @param numCpus
   *          Number of CPUs.
   * @param ram
   *          Amount of RAM.
   * @param disk
   *          Amount of disk.
   * @param numPorts
   *          Number of ports.
   */
  public Resources(double numCpus, Amount<Long, Data> ram, Amount<Long, Data> disk, int numPorts) {
    this.numCpus = numCpus;
    this.ram = requireNonNull(ram);
    this.disk = requireNonNull(disk);
    this.numPorts = numPorts;
  }

  /**
   * Tests whether this bundle of resources is greater than or equal to another bundle of resources.
   *
   * @param other
   *          Resources being compared to.
   * @return {@code true} if all resources in this bundle are greater than or equal to the
   *         equivalents from {@code other}, otherwise {@code false}.
   */
  public boolean greaterThanOrEqual(Resources other) {
    return numCpus >= other.numCpus && disk.as(Data.MB) >= other.disk.as(Data.MB)
        && ram.as(Data.MB) >= other.ram.as(Data.MB) && numPorts >= other.numPorts;
  }

  /**
   * Adapts this resources object to a list of mesos resources.
   *
   * @param selectedPorts
   *          The ports selected, to be applied as concrete task ranges.
   * @return Mesos resources.
   */
  public List<Resource> toResourceList(Set<Integer> selectedPorts) {
    ResourceContext context = ResourceContextHolder.getResourceContext();
    Preconditions.checkNotNull(context);
    List<TrackableResource> offeredResources = context.getTrackableResources();
    ImmutableList.Builder<Resource> resourceBuilder = ImmutableList.<Resource> builder();
    double leftNumCpus = numCpus;
    double leftDisk = disk.as(Data.MB);
    double leftRam = ram.as(Data.MB);
    Set<Integer> leftPorts = Sets.newHashSet(selectedPorts);

    for (TrackableResource resource : offeredResources) {
      switch (resource.getResource().getName()) {
        case CPUS:
          if (leftNumCpus > 0) {
            leftNumCpus -= allocateSclarResource(CPUS, resource, leftNumCpus, resourceBuilder);
          }
          break;
        case DISK_MB:
          if (leftDisk > 0) {
            leftDisk -= allocateSclarResource(DISK_MB, resource, leftDisk, resourceBuilder);
          }
          break;
        case RAM_MB:
          if (leftRam > 0) {
            leftRam -= allocateSclarResource(RAM_MB, resource, leftRam, resourceBuilder);
          }
          break;
        case PORTS:
          if (!selectedPorts.isEmpty()) {
            Set<Integer> allocatedPorts = allocatePortResource(resource, selectedPorts,
                resourceBuilder);
            leftPorts.removeAll(allocatedPorts);
            LOG.info("----allocated port:" + allocatedPorts);
          }
          break;
        default:
          break;
      }
    }

    return resourceBuilder.build();
  }

  /**
   * allocate scalar resource.
   * @param key
   * @param resource
   * @param left
   * @param resourceBuilder
   * @return actually allocated resource
   */
  private double allocateSclarResource(String key, TrackableResource resource, double left,
      ImmutableList.Builder<Resource> resourceBuilder) {
    double allocated = resource.allocateFromScalar(left);

    LOG.info("resource=" + resource + ",name=" + resource.getResource().getName() + ",role="
        + resource.getRole() + ", value=" + resource.getResource().getScalar().getValue());
    if (allocated > 0) {
      resourceBuilder.add(Resources.makeMesosResource(key, allocated, resource.getResource()
          .getRole()));
    }
    return allocated;
  }

  /**
   * only range type is accept when filtering, so support range only.
   *
   * @param resource
   * @param selectedPorts
   */
  private Set<Integer> allocatePortResource(TrackableResource resource, Set<Integer> selectedPorts,
      ImmutableList.Builder<Resource> resourceBuilder) {
    Set<Integer> allocatedPorts = Sets.newHashSet();
    Ranges portRange = resource.getResource().getRanges();
    PeekingIterator<Integer> iterator = Iterators.peekingIterator(Sets.newTreeSet(selectedPorts)
        .iterator());
    Range currentRange = null;
    Ranges.Builder builder = Ranges.newBuilder();

    while (iterator.hasNext()) {
      int start = iterator.next();
      int end = start;
      currentRange = getRangeBelongTo(start, portRange);
      if (currentRange == null) { // port is not in this resource
        continue;
      }
      allocatedPorts.add(start);
      while (iterator.hasNext() && iterator.peek() == end + 1 && inRange(end, currentRange)) {
        end++;
        allocatedPorts.add(end);
        iterator.next();
      }
      // builder.add(com.google.common.collect.Range.closed(start, end));
      builder.addRange(Range.newBuilder().setBegin(start).setEnd(end).build());
      LOG.info("----add range: begin=" + start + ";end=" + end);
    }
    if (builder.getRangeCount() > 0) {
      resourceBuilder.add(Resource.newBuilder().setName(PORTS).setType(Type.RANGES)
          .setRole(resource.getResource().getRole()).setRanges(builder.build()).build());
    }
    return allocatedPorts;
  }

  private Range getRangeBelongTo(Integer value, final Ranges ranges) {
    for (Range range : ranges.getRangeList()) {
      long offerRangeBegin = range.getBegin();
      long offerRangeEnd = range.getEnd();
      if (value >= offerRangeBegin && value <= offerRangeEnd) {
        return range;
      }
    }
    return null;
  }

  private boolean inRange(Integer value, Range range) {
    if (value >= range.getBegin() && value <= range.getEnd()) {
      return true;
    }
    return false;
  }

  /**
   * Convenience method for adapting to mesos resources without applying a port range.
   *
   * @see {@link #toResourceList(java.util.Set)}
   * @return Mesos resources.
   */
  public List<Resource> toResourceList() {
    return toResourceList(ImmutableSet.<Integer> of());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Resources)) {
      return false;
    }

    Resources other = (Resources) o;
    return Objects.equals(numCpus, other.numCpus) && Objects.equals(ram, other.ram)
        && Objects.equals(disk, other.disk) && Objects.equals(numPorts, other.numPorts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(numCpus, ram, disk, numPorts);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this).add("numCpus", numCpus)
        .add("ram", ram).add("disk", disk).add("numPorts", numPorts).toString();
  }

  /**
   * Extracts the resources required from a task.
   *
   * @param task
   *          Task to get resources from.
   * @return The resources required by the task.
   */
  public static Resources from(ITaskConfig task) {
    requireNonNull(task);
    return new Resources(task.getNumCpus(), Amount.of(task.getRamMb(), Data.MB), Amount.of(
        task.getDiskMb(), Data.MB), task.getRequestedPorts().size());
  }

  /**
   * Extracts the resources specified in a list of resource objects.
   *
   * @param resources
   *          Resources to translate.
   * @return The canonical resources.
   */
  public static Resources from(List<Resource> resources) {
    requireNonNull(resources);
    return new Resources(getScalarValue(resources, CPUS), Amount.of(
        (long) getScalarValue(resources, RAM_MB), Data.MB), Amount.of(
            (long) getScalarValue(resources, DISK_MB), Data.MB), getNumAvailablePorts(resources));
  }

  /**
   * Extracts the resources available in a slave offer.
   *
   * @param offer
   *          Offer to get resources from.
   * @return The resources available in the offer.
   */
  public static Resources from(Offer offer) {
    requireNonNull(offer);
    return new Resources(getSumScalarValue(offer, CPUS), Amount.of(
        (long) getSumScalarValue(offer, RAM_MB), Data.MB), Amount.of(
            (long) getSumScalarValue(offer, DISK_MB), Data.MB),
            getNumAvailablePorts(offer.getResourcesList()));
  }

  /**
   * because in multiple role case, same resource type may has more than one resource in offer, such
   * as one resource with role "*", one resource with role "aurora". The offered resource should be
   * the sum value
   *
   * @return
   */
  private static double getSumScalarValue(Offer offer, String key) {
    return getSumScalarValue(offer.getResourcesList(), key);
  }

  private static double getSumScalarValue(List<Resource> resources, String key) {
    double retval = 0;
    for (Resource resource : resources) {
      if (resource.getName().equals(key)) {
        retval += resource.getScalar().getValue();
      }
    }
    return retval;
  }

  @VisibleForTesting
  public static final Resources NONE = new Resources(0, Amount.of(0L, Data.BITS), Amount.of(0L,
      Data.BITS), 0);

  /**
   * a - b.
   */
  public static Resources subtract(Resources a, Resources b) {
    return new Resources(a.getNumCpus() - b.getNumCpus(), Amount.of(a.getRam().as(Data.MB)
        - b.getRam().as(Data.MB), Data.MB), Amount.of(
            a.getDisk().as(Data.MB) - b.getDisk().as(Data.MB), Data.MB), a.getNumPorts()
            - b.getNumPorts());
  }

  /**
   * sum(a, b).
   */
  public static Resources sum(Resources a, Resources b) {
    return sum(ImmutableList.of(a, b));
  }

  /**
   * sum(rs).
   */
  public static Resources sum(Iterable<Resources> rs) {
    Resources sum = NONE;

    for (Resources r : rs) {
      double numCpus = sum.getNumCpus() + r.getNumCpus();
      Amount<Long, Data> disk = Amount.of(
          sum.getDisk().as(Data.BYTES) + r.getDisk().as(Data.BYTES), Data.BYTES);
      Amount<Long, Data> ram = Amount.of(sum.getRam().as(Data.BYTES) + r.getRam().as(Data.BYTES),
          Data.BYTES);
      int ports = sum.getNumPorts() + r.getNumPorts();
      sum = new Resources(numCpus, ram, disk, ports);
    }

    return sum;
  }

  private static int getNumAvailablePorts(List<Resource> resource) {
    int offeredPorts = 0;
    for (Range range : getPortRanges(resource)) {
      offeredPorts += 1 + range.getEnd() - range.getBegin();
    }
    return offeredPorts;
  }

  private static double getScalarValue(List<Resource> resources, String key) {
    Resource resource = getResource(resources, key);
    if (resource == null) {
      return 0;
    }

    return resource.getScalar().getValue();
  }

  private static Resource getResource(List<Resource> resource, String key) {
    return Iterables.find(resource, withName(key), null);
  }

  private static Predicate<Resource> withName(final String name) {
    return new Predicate<Resource>() {
      @Override
      public boolean apply(Resource resource) {
        return resource.getName().equals(name);
      }
    };
  }

  private static Iterable<Range> getPortRanges(List<Resource> resources) {
    List<Range> rangeList = Lists.newLinkedList();
    for (Resource resource : resources) {
      if (Resources.PORTS.equals(resource.getName())) {
        rangeList.addAll(resource.getRanges().getRangeList());
      }
    }
    return rangeList;
    // Resource resource = getResource(resources, Resources.PORTS);
    // if (resource == null) {
    // return ImmutableList.of();
    // }
    //
    // return resource.getRanges().getRangeList();
  }

  /**
   * Creates a scalar mesos resource.
   *
   * @param name
   *          Name of the resource.
   * @param value
   *          Value for the resource.
   * @return A mesos resource.
   */
  public static Resource makeMesosResource(String name, double value) {
    return Resource.newBuilder().setName(name).setType(Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(value)).build();
  }

  /**
   * Creates a scalar mesos resource.
   *
   * @param name
   *          Name of the resource.
   * @param value
   *          Value for the resource.
   * @param role
   *          for the resource
   * @return A mesos resource.
   */
  public static Resource makeMesosResource(String name, double value, String role) {
    return Resource.newBuilder().setName(name).setType(Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(value)).setRole(role).build();
  }

  private static final Function<com.google.common.collect.Range<Integer>, Range> RANGE_TRANSFORM =
      new Function<com.google.common.collect.Range<Integer>, Range>() {
    @Override
    public Range apply(com.google.common.collect.Range<Integer> input) {
      return Range.newBuilder().setBegin(input.lowerEndpoint()).setEnd(input.upperEndpoint())
          .build();
    }
  };

  /**
   * Creates a mesos resource of integer ranges.
   *
   * @param name
   *          Name of the resource
   * @param values
   *          Values to translate into ranges.
   * @return A mesos ranges resource.
   */
  @VisibleForTesting
  public static Resource makeMesosRangeResource(String name, Set<Integer> values, String role) {
    return Resource
        .newBuilder()
        .setName(name)
        .setType(Type.RANGES)
        .setRole(role)
        .setRanges(
            Ranges.newBuilder().addAllRange(
                Iterables.transform(Numbers.toRanges(values), RANGE_TRANSFORM))).build();
  }

  /**
   * Number of CPUs.
   *
   * @return CPUs.
   */
  public double getNumCpus() {
    return numCpus;
  }

  /**
   * Disk amount.
   *
   * @return Disk.
   */
  public Amount<Long, Data> getDisk() {
    return disk;
  }

  /**
   * RAM amount.
   *
   * @return RAM.
   */
  public Amount<Long, Data> getRam() {
    return ram;
  }

  /**
   * Number of ports.
   *
   * @return Port count.
   */
  public int getNumPorts() {
    return numPorts;
  }

  /**
   * Thrown when there are insufficient resources to satisfy a request.
   */
  static class InsufficientResourcesException extends RuntimeException {
    public InsufficientResourcesException(String message) {
      super(message);
    }
  }

  /**
   * Attempts to grab {@code numPorts} from the given resource {@code offer}.
   *
   * @param offer
   *          The offer to grab ports from.
   * @param numPorts
   *          The number of ports to grab.
   * @return The set of ports grabbed.
   * @throws InsufficientResourcesException
   *           if not enough ports were available.
   */
  public static Set<Integer> getPorts(Offer offer, int numPorts)
      throws InsufficientResourcesException {

    requireNonNull(offer);

    if (numPorts == 0) {
      return ImmutableSet.of();
    }

    List<Integer> availablePorts = Lists.newArrayList(Sets.newHashSet(Iterables.concat(Iterables
        .transform(getPortRanges(offer.getResourcesList()), RANGE_TO_MEMBERS))));

    if (availablePorts.size() < numPorts) {
      throw new InsufficientResourcesException(String.format("Could not get %d ports from %s",
          numPorts, offer));
    }

    Collections.shuffle(availablePorts);
    return ImmutableSet.copyOf(availablePorts.subList(0, numPorts));
  }

  /**
   * Attempts to grab {@code numPorts} from the given resource {@code offer}.
   *
   * @param offer
   *          The offer to grab ports from.
   * @param numPorts
   *          The number of ports to grab.
   * @return The set of ports grabbed.
   * @throws InsufficientResourcesException
   *           if not enough ports were available.
   */
  public static Set<Integer> allocatePorts(int numPorts) throws InsufficientResourcesException {
    if (numPorts == 0) {
      return ImmutableSet.of();
    }

    Set<Integer> allocatedPorts = doAllocatePorts(numPorts);
    if (allocatedPorts.size() < numPorts) {
      throw new InsufficientResourcesException(String.format("Could not get %d ports",
          numPorts));
    }
    return allocatedPorts;
  }

  private static Set<Integer> doAllocatePorts(int numPorts) {
    Set<Integer> ports = Sets.newHashSet();
    int i = numPorts;
    List<TrackableResource> trackableResources = ResourceContextHolder.getResourceContext()
        .getTrackableResources();
    while (i > 0) {
      for (TrackableResource resource : trackableResources) {
        if (PORTS.equals(resource.getResource().getName())) {
          Optional<Long> port = resource.allocateFromRange();
          if (port.isPresent()) {
            ports.add(port.get().intValue());
            break;
          }
        }
      }
      i--;
    }
    return ports;
  }

  /**
   * A Resources object is greater than another iff _all_ of its resource components are greater or
   * equal. A Resources object compares as equal if some but not all components are greater than or
   * equal to the other.
   */
  public static final Ordering<Resources> RESOURCE_ORDER = new Ordering<Resources>() {
    @Override
    public int compare(Resources left, Resources right) {
      int diskC = left.getDisk().compareTo(right.getDisk());
      int ramC = left.getRam().compareTo(right.getRam());
      int portC = Integer.compare(left.getNumPorts(), right.getNumPorts());
      int cpuC = Double.compare(left.getNumCpus(), right.getNumCpus());

      FluentIterable<Integer> vector = FluentIterable.from(ImmutableList.of(diskC, ramC, portC,
          cpuC));

      if (vector.allMatch(IS_ZERO)) {
        return 0;
      }

      if (vector.filter(Predicates.not(IS_ZERO)).allMatch(IS_POSITIVE)) {
        return 1;
      }

      if (vector.filter(Predicates.not(IS_ZERO)).allMatch(IS_NEGATIVE)) {
        return -1;
      }

      return 0;
    }
  };

  private static final Predicate<Integer> IS_POSITIVE = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer input) {
      return input > 0;
    }
  };

  private static final Predicate<Integer> IS_NEGATIVE = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer input) {
      return input < 0;
    }
  };

  private static final Predicate<Integer> IS_ZERO = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer input) {
      return input == 0;
    }
  };
}
