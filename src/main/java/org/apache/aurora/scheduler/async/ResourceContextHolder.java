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

public class ResourceContextHolder {
  private static final ThreadLocal<ResourceContext> context = new ThreadLocal<ResourceContext>();

  public static void setResourceContext(ResourceContext resourceContext) {
    context.set(resourceContext);
  }

  public static ResourceContext getResourceContext() {
    return context.get();
  }

  public static void clear() {
    context.remove();
  }

}
