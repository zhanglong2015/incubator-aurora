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

import org.apache.mesos.Protos.Resource;

public class TrackableResource {
    private Resource resource;
    private double left = 0;

    public TrackableResource(Resource resource) {
        this.resource = resource;
        this.left = resource.getScalar().getValue();
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public boolean allocatable() {
        return left > 0;
    }

    public double getAvailableResource() {
        return left;
    }

    public void allocate(double allocated) {
        this.left -= allocated;
    }

    public String getRole() { return  resource.getRole();}

}
