package org.apache.aurora.scheduler.async;

import java.util.LinkedList;
import java.util.List;

import org.apache.aurora.scheduler.mesos.TrackableResource;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

public class TaskContext {
	private List<TrackableResource> trackableResource = new LinkedList<TrackableResource>();
	
	public TaskContext(Offer offer) {
    List<Resource> resources = offer.getResourcesList();
    for (Resource resource : resources) {
    	trackableResource.add(new TrackableResource(resource));
    }
	}
	
  public List<TrackableResource> getTrackableResources() {
  	return trackableResource;
  }
}
