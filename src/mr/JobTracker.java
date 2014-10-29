/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.Remote;

/**
 * @author Nicolas_Yu
 *
 */
public interface JobTracker extends Serializable, Remote{
	
	public void initialize();
	
	public void schedule(Job job);
	
	public void sendHeartbeat();
	
	public void describeJobs();
	
	public void describeJob(Job job);
	
	public void register(String hostId, TaskTracker taskTracker);
	
	/*
	 * terminate mapreduce framework
	 */
	public void terminate();
	
	public void kill();
	
	public void kill(String jobId);
}
