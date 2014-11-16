/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

/**
 * @author Nicolas_Yu
 * 
 */
public interface JobTracker extends Serializable, Remote {

	/**
	 * Initialize JobTracker with MR Registry
	 * 
	 * @param mrPort
	 * @param hdfsRegistryHost
	 * @param hdfsPort
	 * @param jobTrackerPort
	 * @param reducerNum
	 * @throws RemoteException
	 */
	public void initialize(int mrPort, String hdfsRegistryHost,
			int hdfsPort, int jobTrackerPort, int reducerNum)
			throws RemoteException;

	/**
	 * Schedule a new Job in JobTracker
	 * 
	 * @param job
	 * @throws RemoteException
	 */
	public void schedule(Job job) throws RemoteException;

	/**
	 * Check status of taskTrackers using heart beat
	 */
	public void checkHeartbeat(Message message) throws RemoteException;

	/**
	 * Describe current jobs' status
	 * 
	 * @return
	 * @throws RemoteException
	 */
	public String describeJobs() throws RemoteException;

	/**
	 * Describe one job's status
	 * 
	 * @param job
	 * @return
	 */
	public String describeJob(String jobId) throws RemoteException;

	/**
	 * Register taskTrcker to JobTracker
	 * 
	 * @param hostId
	 * @param taskTracker
	 */
	public void register(String hostId, TaskTracker taskTracker);

	public void healthCheck();

	public void allocateMapper(String hostId, String mapId, String blockId,
			String readFromHost, Job job,
			java.util.Hashtable<String, String> hostMapper)
			throws RemoteException;

	/**
	 * Kill one job using JobId
	 * 
	 * @param jobId
	 * @throws RemoteException
	 */
	public void kill(String jobId) throws RemoteException;

	/**
	 * @param jobID
	 * @return
	 */
	HashMap<String, List<String>> chooseReducer(String jobID);

	/**
	 * @param jobId
	 * @param writePath
	 * @param hostID_hashIDs
	 */
	void startReducer(String jobId, String writePath,
			HashMap<String, List<String>> hostID_hashIDs);

	/**
	 * @throws RemoteException
	 */
	void terminate() throws RemoteException;
}
