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
package org.apache.aurora.scheduler.async;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Ordering;

import org.apache.aurora.scheduler.mesos.TrackableResource;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

public class ResourceContext {
  private final List<TrackableResource> trackableResource;
  private static final String DEFAULT_MESOS_ROLE = "*";

  public ResourceContext(Offer offer) {
    List<TrackableResource> tmp = new LinkedList<TrackableResource>();
    List<Resource> resources = offer.getResourcesList();
    for (Resource resource : resources) {
      tmp.add(new TrackableResource(resource));
    }
    trackableResource = ROLE_FIRST.sortedCopy(tmp);
  }

  public List<TrackableResource> getTrackableResources() {
    return trackableResource;
  }

  private static final Ordering<TrackableResource> ROLE_FIRST = Ordering.from(
      new Comparator<TrackableResource>() {
        @Override
        public int compare(TrackableResource r0, TrackableResource r1) {
          if (r0.getResource().getRole().equals(r1.getResource().getRole())) {
            return r0.getResource().getName().compareTo(r1.getResource().getName());
          }
          if (DEFAULT_MESOS_ROLE.equals(r0.getResource().getRole())) {
            return 1;
          } else {
            return -1;
          }
        }
      });
}
