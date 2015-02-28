package org.apache.aurora.scheduler.async;

public class TaskContextHolder {
	private static final ThreadLocal<TaskContext> context = new ThreadLocal<TaskContext>();
	
	public static void setContext(TaskContext taskContext) {
		context.set(taskContext);
	}
	
	public static TaskContext getContext() {
		return context.get();
	}
	
	public static void clear() {
		context.remove();
	}
	
}
