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
package org.apache.aurora.scheduler.http;

import javax.inject.Inject;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;

/**
 * A servlet that exposes the scheduler Thrift API over HTTP/JSON.
 */
class SchedulerAPIServlet extends TServlet {

  @Inject
  SchedulerAPIServlet(AuroraAdmin.Iface schedulerThriftInterface) {
    super(new AuroraAdmin.Processor<>(schedulerThriftInterface), new TJSONProtocol.Factory());
  }
}
