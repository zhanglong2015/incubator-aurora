package org.apache.aurora.scheduler.async;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Ordering;

import org.apache.aurora.scheduler.mesos.TrackableResource;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

public class ResourceContext {
	private List<TrackableResource> trackableResource = new LinkedList<TrackableResource>();
	private static final String DEFAULT_MESOS_ROLE = "*";
	
	public ResourceContext(Offer offer) {
    List<Resource> resources = offer.getResourcesList();    
    for (Resource resource : resources) {
    	trackableResource.add(new TrackableResource(resource));
    }
    trackableResource = ROLE_FIRST.sortedCopy(trackableResource);
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
