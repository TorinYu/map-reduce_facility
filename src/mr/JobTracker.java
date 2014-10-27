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
	
	public void describe_jobs();
	
	public void describe_job(Job job);
	
	public void terminate();
	
	public void kill();
	
	public void kill(String jobId);
}
