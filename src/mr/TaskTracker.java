/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Nicolas_Yu
 *
 */
public interface TaskTracker extends Serializable, Remote{
	
	public void setReduceNum(int reduceNum);
	
	public void startMapper();
	
	public void startReducer();
	
	public void writeFile(String path, byte[] content);
	
	public String readFile(String path, String name);
	
	public void heartBeat() throws RemoteException;

	/**
	 * @param job_id
	 * @param reducerId
	 * @param writePath
	 * @param reducer
	 * @param clspath
	 */
	void startMapper(String job_id, String reducerId, String writePath,
			Class<? extends Reducer> reducer, String clspath);

	/**
	 * @param job_id
	 * @param reducerId
	 * @param writePath
	 * @param reducer
	 * @param clspath
	 */
	void startReducer(String job_id, String reducerId, String writePath,
			Class<? extends Reducer> reducer, String clspath);
	
	
}
