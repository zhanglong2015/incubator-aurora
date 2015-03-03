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
