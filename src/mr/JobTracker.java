/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.Remote;
import java.util.HashMap;

import com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable;

/**
 * @author Nicolas_Yu
 *
 */
public interface JobTracker extends Serializable, Remote{
	
	public void initialize(String hdfsRegistryHost, int hdfsPort, int mapReducePort,
			int jobTrackerPort, int reduceNum);
	
	public void schedule(Job job);
	
	public void sendHeartbeat();
	
	public void describeJobs();
	
	public void describeJob(Job job);
	
	public void register(String hostId, TaskTracker taskTracker);
	
	public void healthCheck();
	
	public void allocateMapper(String hostId, String mapId, String blockId, String readFromHost, Job job, HashMap<String, String> hostMapper);
	
	/*
	 * terminate mapreduce framework
	 */
	public void terminate();
	
	public void kill(String jobId);

	/**
	 * @param hdfsRegistryHost
	 * @param hdfsPort
	 * @param mapReducePort
	 * @param jobTrackerPort
	 */
	
}
