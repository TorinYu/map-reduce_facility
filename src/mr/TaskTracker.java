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
public interface TaskTracker extends Serializable, Remote{
	
	public void setReduceNum();
	
	public void startMapper();
	
	public void startReducer();
	
	public void writeFile(String path, String content);
	
	public String readFile(String path, String name);
	
	public void sendHeartbeat();

	/**
	 * @param job_id
	 * @param reducer_id
	 * @param write_path
	 * @param reducer
	 * @param clspath
	 */
	void startMapper(String job_id, String reducer_id, String write_path,
			Class<? extends Reducer> reducer, String clspath);
	
	
}
